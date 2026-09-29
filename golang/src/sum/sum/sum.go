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
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		return nil, errors.Join(err, inputQueue.Close())
	}

	controlExchangeKeys := []string{"control_broadcast", fmt.Sprintf("control_node_%d", config.Id)}
	controlExchange, err := middleware.CreateExchangeMiddleware(fmt.Sprintf("control_node_%s", config.SumPrefix), controlExchangeKeys, connSettings)
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
	defer ack()

	clientID, fruitRecords, isEof, totalCount, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		slog.Info("Got EOF from gateway. Coordinator established.", "clientID", clientID)
		err := sum.broadcastControlToken(clientID, totalCount)
		if err != nil {
			slog.Error("While broadcasting control token", "err", err)
			return
		}
		return
	}

	if err := sum.handleDataMessage(clientID, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleDataMessage(clientID uint64, fruitRecords []fruititem.FruitItem) error {
	sum.mapsLock.Lock()
	if _, exists := sum.clientFruitItemMap[clientID]; !exists {
		sum.clientFruitItemMap[clientID] = map[string]fruititem.FruitItem{}
	}
	for _, fruitRecord := range fruitRecords {
		_, ok := sum.clientFruitItemMap[clientID][fruitRecord.Fruit]
		if ok {
			sum.clientFruitItemMap[clientID][fruitRecord.Fruit] = sum.clientFruitItemMap[clientID][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.clientFruitItemMap[clientID][fruitRecord.Fruit] = fruitRecord
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
	return nil
}

func (sum *Sum) handleControlToken(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	controlToken, err := inner.DeserializeControlToken(&msg)
	if err != nil {
		slog.Error("While deserializing control token", "err", err)
		return
	}

	if controlToken.CoordinatorID == sum.id {
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
				return
			}

			if err := sum.controlExchange.SendTo(*msg, "control_broadcast"); err != nil {
				slog.Error("While sending END control message", "err", err)
				return
			}
		}
		return
	}

	if controlToken.Flag == inner.COORD {
		sum.mapsLock.Lock()
		sum.clientCoordinatorId[controlToken.ClientID] = controlToken.CoordinatorID
		processed := sum.clientMessageCounter[controlToken.ClientID]
		sum.clientMessageCounter[controlToken.ClientID] = 0
		sum.mapsLock.Unlock()

		countToken := inner.ControlToken{ClientID: controlToken.ClientID, Flag: inner.COUNT, CoordinatorID: controlToken.CoordinatorID, CurrentCount: processed}
		if err := sum.sendControlTokenTo(controlToken.CoordinatorID, &countToken); err != nil {
			slog.Error("While sending COUNT control message", "err", err)
		}
	} else if controlToken.Flag == inner.FINAL_ROUND {
		sum.mapsLock.Lock()
		delete(sum.clientMessageCounter, controlToken.ClientID)
		delete(sum.clientCoordinatorId, controlToken.ClientID)
		sum.mapsLock.Unlock()

		if err := sum.flushFruitSums(controlToken.ClientID); err != nil {
			slog.Error("While sending client records", "err", err)
			return
		}
		if err := sum.sendEOF(controlToken.ClientID); err != nil {
			slog.Error("While sending end of the-records message", "err", err)
			return
		}
	}
	return
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
		batch := sharding(fruitRecord.Fruit, sum.aggregationAmount)
		batches[batch] = append(batches[batch], fruitRecord)
	}

	for batch, fruitRecords := range batches {
		msg, err := inner.SerializeMessage(clientID, fruitRecords)
		if err != nil {
			return err
		}
		routingKey := fmt.Sprintf("%s_%d", sum.aggregationPrefix, batch)
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
	return sum.controlExchange.SendTo(*message, fmt.Sprintf("control_node_%d", id))
}

func (sum *Sum) broadcastControlToken(clientID uint64, totalCount uint64) error {
	sum.mapsLock.Lock()
	sum.remainingClientMsg[clientID] = totalCount - sum.clientMessageCounter[clientID]
	delete(sum.clientMessageCounter, clientID)
	sum.mapsLock.Unlock()

	err := sum.flushFruitSums(clientID)
	if err != nil {
		slog.Error("While flushing", "err", err)
		return err
	}

	err = sum.sendEOF(clientID)
	if err != nil {
		slog.Error("While sending EOF", "err", err)
		return err
	}

	token := inner.ControlToken{ClientID: clientID, Flag: inner.COORD, CoordinatorID: sum.id}
	msg, err := inner.SerializeControlToken(&token)
	if err != nil {
		slog.Error("While serializing control token", "err", err)
		return err
	}
	err = sum.controlExchange.SendTo(*msg, "control_broadcast")
	if err != nil {
		slog.Error("While broadcasting token as coordinator", "err", err)
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

func sharding(fruitName string, aggregationAmount int) int {
	h := fnv.New32a()
	_, err := h.Write([]byte(fruitName))
	if err != nil {
		slog.Info("Error sharding fruit", "err", err)
		return 0
	}
	targetIndex := h.Sum32() % uint32(aggregationAmount)
	return int(targetIndex)
}

func (sum *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	sum.running.Store(false)
	_ = sum.inputQueue.StopConsuming()
	_ = sum.controlExchange.StopConsuming()
}

func (sum *Sum) closeMiddlewares() error {
	return errors.Join(
		sum.inputQueue.StopConsuming(),
		sum.controlExchange.StopConsuming(),
		sum.inputQueue.Close(),
		sum.outputExchange.Close(),
		sum.controlExchange.Close())
}
