package join

import (
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue           middleware.Middleware
	outputQueue          middleware.Middleware
	topSize              int
	expectedEOFs         int
	receivedEOFs         map[uint64]int
	clientAccumulatedTop map[uint64][]fruititem.FruitItem
	running              atomic.Bool
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	join := &Join{
		inputQueue:           inputQueue,
		outputQueue:          outputQueue,
		topSize:              config.TopSize,
		expectedEOFs:         config.AggregationAmount,
		receivedEOFs:         map[uint64]int{},
		clientAccumulatedTop: map[uint64][]fruititem.FruitItem{},
	}
	join.running.Store(true)
	return join, nil
}

func (join *Join) Run() error {
	go join.handleSignals()

	consumeErr := join.inputQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		join.handleMessage(msg, ack, nack)
	})
	closeErr := join.closeMiddlewares()

	if closeErr != nil {
		slog.Warn("Middlewares closed with errors", "err", closeErr)
	}
	if !join.running.Load() {
		return nil
	}
	return errors.Join(consumeErr, closeErr)
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	clientID, fruitRecordsTop, isEof, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message in Join", "err", err)
		nack()
		return
	}
	defer ack()

	if isEof {
		if err = join.handleEndOfRecordsMessage(clientID); err != nil {
			slog.Error("While handling End Of Records message in Join", "err", err)
			return
		}
	} else {
		join.updateClientTop(clientID, fruitRecordsTop)
	}
}

func (join *Join) handleEndOfRecordsMessage(clientID uint64) error {
	slog.Info("Received End Of Records message")
	join.receivedEOFs[clientID]++

	if join.receivedEOFs[clientID] < join.expectedEOFs {
		return nil
	}
	return join.sendClientTop(clientID)
}

func (join *Join) sendClientTop(clientID uint64) error {
	finalTop, exists := join.clientAccumulatedTop[clientID]
	if !exists || finalTop == nil {
		finalTop = []fruititem.FruitItem{}
	}
	delete(join.clientAccumulatedTop, clientID)
	delete(join.receivedEOFs, clientID)

	message, err := inner.SerializeMessage(clientID, finalTop)
	if err != nil {
		slog.Error("serializing final top", "err", err)
		return err
	}
	if err := join.outputQueue.Send(*message); err != nil {
		slog.Error("sending final top", "err", err)
		return err
	}
	return nil
}

func (join *Join) updateClientTop(clientID uint64, partialTop []fruititem.FruitItem) {
	accumulatedTop, exists := join.clientAccumulatedTop[clientID]
	if !exists {
		join.clientAccumulatedTop[clientID] = partialTop[:min(join.topSize, len(partialTop))]
		return
	}
	if len(partialTop) == 0 {
		return
	}
	updatedTop := make([]fruititem.FruitItem, min(join.topSize, len(accumulatedTop)+len(partialTop)))

	i, j := 0, 0
	for (i+j < join.topSize) && (i < len(accumulatedTop) || j < len(partialTop)) {
		switch {
		case j >= len(partialTop):
			updatedTop[i+j] = accumulatedTop[i]
			i++
		case i >= len(accumulatedTop):
			updatedTop[i+j] = partialTop[j]
			j++
		case accumulatedTop[i].Less(partialTop[j]):
			updatedTop[i+j] = partialTop[j]
			j++
		default:
			updatedTop[i+j] = accumulatedTop[i]
			i++
		}
	}
	join.clientAccumulatedTop[clientID] = updatedTop
}

func (join *Join) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	join.running.Store(false)
	if err := join.inputQueue.StopConsuming(); err != nil {
		slog.Debug("Error stopping consumers", "err", err)
	}
}

func (join *Join) closeMiddlewares() error {
	return errors.Join(
		join.inputQueue.StopConsuming(),
		join.inputQueue.Close(),
		join.outputQueue.Close())
}
