package sum

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	id                   int
	inputQueue           middleware.Middleware
	outputExchange       middleware.SelectiveSender
	clientFruitItemMap   map[uint64]map[string]fruititem.FruitItem
	controlExchange      middleware.SelectiveSender
	mapsLock             sync.Mutex
	aggregationPrefix    string
	aggregationAmount    int
	clientMessageCounter map[uint64]uint64
	remainingClientMsg   map[uint64]uint64
	clientCoordinatorId  map[uint64]int
	running              atomic.Bool
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf(aggregationKey, config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		return nil, errors.Join(err, inputQueue.Close())
	}

	controlExchangeKeys := []string{controlBroadcastKey, fmt.Sprintf(controlNodeKey, config.Id)}
	controlExchange, err := middleware.CreateExchangeMiddleware(fmt.Sprintf(controlExchangeKey, config.SumPrefix), controlExchangeKeys, connSettings)
	if err != nil {
		return nil, errors.Join(err, inputQueue.Close(), outputExchange.Close())
	}

	sum := &Sum{
		id:                   config.Id,
		inputQueue:           inputQueue,
		outputExchange:       outputExchange,
		clientFruitItemMap:   map[uint64]map[string]fruititem.FruitItem{},
		controlExchange:      controlExchange,
		clientMessageCounter: map[uint64]uint64{},
		remainingClientMsg:   map[uint64]uint64{},
		clientCoordinatorId:  map[uint64]int{},
		aggregationPrefix:    config.AggregationPrefix,
		aggregationAmount:    config.AggregationAmount,
	}
	sum.running.Store(true)
	return sum, nil
}

func (sum *Sum) Run() error {
	go sum.consumeControl()
	go sum.handleSignals()
	consumeErr := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
	closeErr := sum.closeMiddlewares()
	if closeErr != nil {
		slog.Warn("Middlewares closed with errors", "err", closeErr)
	}
	if !sum.running.Load() {
		return nil
	}
	return errors.Join(consumeErr, closeErr)
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	clientID, fruitRecords, isEof, totalCount, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}
	defer ack()
	if isEof {
		slog.Info("Got EOF from gateway. Coordinator established", "clientID", clientID)
		err := sum.broadcastControlToken(clientID, totalCount)
		if err != nil {
			slog.Error("While starting coordination round", "err", err)
			return
		}
		return
	}

	sum.handleDataMessage(clientID, fruitRecords)
}

func (sum *Sum) handleDataMessage(clientID uint64, fruitRecords []fruititem.FruitItem) {
	sum.mapsLock.Lock()
	if _, exists := sum.clientFruitItemMap[clientID]; !exists {
		sum.clientFruitItemMap[clientID] = map[string]fruititem.FruitItem{}
	}
	currentClientMap := sum.clientFruitItemMap[clientID]
	for _, fruitRecord := range fruitRecords {
		_, ok := currentClientMap[fruitRecord.Fruit]
		if ok {
			currentClientMap[fruitRecord.Fruit] = currentClientMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			currentClientMap[fruitRecord.Fruit] = fruitRecord
		}
	}

	sum.clientMessageCounter[clientID]++
	coordID, coordExists := sum.clientCoordinatorId[clientID]
	processed := sum.clientMessageCounter[clientID]
	if coordExists {
		sum.clientMessageCounter[clientID] = 0
	}
	sum.mapsLock.Unlock()

	if coordExists {
		message := inner.ControlToken{ClientID: clientID, Flag: inner.COUNT, CoordinatorID: coordID, CurrentCount: processed}
		if err := sum.sendControlTokenTo(coordID, &message); err != nil {
			slog.Error("While sending COUNT control message", "err", err)
		}
	}
}

func (sum *Sum) handleControlToken(msg middleware.Message, ack func(), nack func()) {
	controlToken, err := inner.DeserializeControlToken(&msg)
	if err != nil {
		slog.Error("While deserializing control token", "err", err)
		nack()
		return
	}
	defer ack()

	switch {
	case controlToken.CoordinatorID == sum.id:
		sum.countAsCoordinator(controlToken)
	case controlToken.Flag == inner.COORD:
		sum.registerCoordinator(controlToken)
	case controlToken.Flag == inner.FINAL_ROUND:
		sum.publishClientTop(controlToken.ClientID)
	}
}

func (sum *Sum) countAsCoordinator(controlToken *inner.ControlToken) {
	if controlToken.Flag != inner.COUNT {
		return
	}
	sum.mapsLock.Lock()
	_, exists := sum.remainingClientMsg[controlToken.ClientID]
	if !exists {
		sum.mapsLock.Unlock()
		slog.Warn("COUNT for already closed client", "clientId", controlToken.ClientID)
		return
	}
	sum.remainingClientMsg[controlToken.ClientID] -= controlToken.CurrentCount
	allMessagesProcessed := sum.remainingClientMsg[controlToken.ClientID] == 0
	if allMessagesProcessed {
		delete(sum.remainingClientMsg, controlToken.ClientID)
	}
	sum.mapsLock.Unlock()

	if allMessagesProcessed {
		finalToken := inner.ControlToken{ClientID: controlToken.ClientID, Flag: inner.FINAL_ROUND, CoordinatorID: sum.id}
		msg, err := inner.SerializeControlToken(&finalToken)
		if err != nil {
			slog.Error("While serializing control token", "err", err)
			return
		}

		if err := sum.controlExchange.SendTo(*msg, controlBroadcastKey); err != nil {
			slog.Error("While sending FINAL_ROUND control message", "err", err)
		}
	}
}

func (sum *Sum) registerCoordinator(controlToken *inner.ControlToken) {
	sum.mapsLock.Lock()
	sum.clientCoordinatorId[controlToken.ClientID] = controlToken.CoordinatorID
	processed := sum.clientMessageCounter[controlToken.ClientID]
	sum.clientMessageCounter[controlToken.ClientID] = 0
	sum.mapsLock.Unlock()

	countToken := inner.ControlToken{ClientID: controlToken.ClientID, Flag: inner.COUNT, CoordinatorID: controlToken.CoordinatorID, CurrentCount: processed}
	if err := sum.sendControlTokenTo(controlToken.CoordinatorID, &countToken); err != nil {
		slog.Error("While sending COUNT control message", "err", err)
	}
}

func (sum *Sum) publishClientTop(clientID uint64) {
	sum.mapsLock.Lock()
	delete(sum.clientMessageCounter, clientID)
	delete(sum.clientCoordinatorId, clientID)
	sum.mapsLock.Unlock()

	if err := sum.flushFruitSums(clientID); err != nil {
		slog.Error("While sending client records", "err", err)
		return
	}
	if err := sum.sendEOF(clientID); err != nil {
		slog.Error("While sending end of the-records message", "err", err)
		return
	}
}

func (sum *Sum) flushFruitSums(clientID uint64) error {
	sum.mapsLock.Lock()
	fruits, exists := sum.clientFruitItemMap[clientID]
	delete(sum.clientFruitItemMap, clientID)
	delete(sum.clientMessageCounter, clientID)
	sum.mapsLock.Unlock()

	if !exists {
		return nil
	}
	batches := map[int][]fruititem.FruitItem{}
	for _, fruitRecord := range fruits {
		batch := sharding(fruitRecord.Fruit, sum.aggregationAmount, clientID)
		batches[batch] = append(batches[batch], fruitRecord)
	}

	for batch, fruitRecords := range batches {
		msg, err := inner.SerializeMessage(clientID, fruitRecords)
		if err != nil {
			return err
		}
		routingKey := fmt.Sprintf(aggregationKey, sum.aggregationPrefix, batch)
		if err := sum.outputExchange.SendTo(*msg, routingKey); err != nil {
			return err
		}
	}
	return nil
}

func (sum *Sum) sendEOF(clientID uint64) error {
	message, err := inner.SerializeEOF(clientID, 0)
	if err != nil {
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		return err
	}
	return nil
}

func (sum *Sum) sendControlTokenTo(id int, token *inner.ControlToken) error {
	message, err := inner.SerializeControlToken(token)
	if err != nil {
		return err
	}
	return sum.controlExchange.SendTo(*message, fmt.Sprintf(controlNodeKey, id))
}

func (sum *Sum) broadcastControlToken(clientID uint64, totalCount uint64) error {
	sum.mapsLock.Lock()
	sum.remainingClientMsg[clientID] = totalCount - sum.clientMessageCounter[clientID]
	delete(sum.clientMessageCounter, clientID)
	sum.mapsLock.Unlock()

	if err := sum.flushFruitSums(clientID); err != nil {
		slog.Error("flushing fruit sums", "err", err)
		return err
	}

	if err := sum.sendEOF(clientID); err != nil {
		slog.Error("sending EOF", "err", err)
		return err
	}

	token := inner.ControlToken{ClientID: clientID, Flag: inner.COORD, CoordinatorID: sum.id}
	msg, err := inner.SerializeControlToken(&token)
	if err != nil {
		slog.Error("serializing COORD token", "err", err)
		return err
	}
	err = sum.controlExchange.SendTo(*msg, controlBroadcastKey)
	if err != nil {
		slog.Error("broadcasting COORD token", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) consumeControl() {
	if err := sum.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleControlToken(msg, ack, nack)
	}); err != nil && sum.running.Load() {
		slog.Error("Sum control consumer stopped with error", "err", err)
	}
}

func sharding(fruitName string, aggregationAmount int, clientID uint64) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(fmt.Sprintf(shardKey, fruitName, clientID)))
	targetIndex := h.Sum32() % uint32(aggregationAmount)
	return int(targetIndex)
}

func (sum *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	sum.running.Store(false)
	if err := errors.Join(
		sum.inputQueue.StopConsuming(),
		sum.controlExchange.StopConsuming(),
	); err != nil {
		slog.Debug("Error stopping consumers", "err", err)
	}
}

func (sum *Sum) closeMiddlewares() error {
	return errors.Join(
		sum.inputQueue.StopConsuming(),
		sum.controlExchange.StopConsuming(),
		sum.inputQueue.Close(),
		sum.outputExchange.Close(),
		sum.controlExchange.Close())
}
