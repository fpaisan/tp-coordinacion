package sum

import (
	"fmt"
	"log/slog"

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
	outputExchange       middleware.Middleware
	clientFruitItemMap   map[uint64]map[string]fruititem.FruitItem
	controlInputQueue    middleware.Middleware
	controlOutputQueue   middleware.Middleware
	clientMessageCounter map[uint64]uint64
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
		inputQueue.Close()
		return nil, err
	}

	controlQueueName := fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)
	controlInputQueue, err := middleware.CreateQueueMiddleware(controlQueueName, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}
	nextSumID := (config.Id + 1) % config.SumAmount
	nextControlQueueName := fmt.Sprintf("%s_%d", config.SumPrefix, nextSumID)
	controlOutputQueue, err := middleware.CreateQueueMiddleware(nextControlQueueName, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		controlInputQueue.Close()
		return nil, err
	}
	return &Sum{
		id:                   config.Id,
		inputQueue:           inputQueue,
		outputExchange:       outputExchange,
		clientFruitItemMap:   map[uint64]map[string]fruititem.FruitItem{},
		controlInputQueue:    controlInputQueue,
		controlOutputQueue:   controlOutputQueue,
		clientMessageCounter: map[uint64]uint64{},
	}, nil
}

func (sum *Sum) Run() {
	go sum.controlInputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleControlToken(msg, ack, nack)
	})
	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientID, fruitRecords, isEof, totalCount, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		slog.Info("Got EOF from gateway, initiating ring cycle...", "clientID", clientID)
		sum.passToken(clientID, totalCount, 0, false, -1)
		return
	}

	if err := sum.handleDataMessage(clientID, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleDataMessage(clientID uint64, fruitRecords []fruititem.FruitItem) error {
	if _, exists := sum.clientFruitItemMap[clientID]; !exists {
		sum.clientFruitItemMap[clientID] = map[string]fruititem.FruitItem{}
	}
	sum.clientMessageCounter[clientID]++
	currentClientMap := sum.clientFruitItemMap[clientID]
	for _, fruitRecord := range fruitRecords {
		_, ok := currentClientMap[fruitRecord.Fruit]
		if ok {
			currentClientMap[fruitRecord.Fruit] = currentClientMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			currentClientMap[fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}

func (sum *Sum) handleControlToken(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	token, err := inner.DeserializeToken(&msg)
	if err != nil {
		slog.Error("While deserializing token", "err", err)
		return
	}

	if token.IsFinalRound {
		if token.CoordinatorID == sum.id {
			slog.Info("Ring cycle complete, sending final EOF to aggregators...", "clientID", token.CoordinatorID)
			eofMessage, err := inner.SerializeEOF(token.ClientID, token.TotalTarget)
			if err != nil {
				slog.Error("While serializing EOF token", "err", err)
				return
			}
			err = sum.outputExchange.Send(*eofMessage)
			if err != nil {
				slog.Error("While sending EOF", "err", err)
				return
			}
			return
		}
		slog.Info("Received token of last round, initiating flushing process...", "clientID", token.ClientID)
		err := sum.flushFruitSums(token.ClientID)
		if err != nil {
			slog.Error("While flushing fruit sums", "err", err)
			return
		}
		sum.passToken(token.ClientID, token.TotalTarget, token.CurrentCount, true, token.CoordinatorID)
		return
	}

	newContribution := sum.clientMessageCounter[token.ClientID]
	newAccumulated := token.CurrentCount + newContribution
	sum.clientMessageCounter[token.ClientID] = 0

	if newAccumulated == token.TotalTarget {
		slog.Info("Target count reached. Coordinator initiating final round...", "clientID", token.ClientID)
		err := sum.flushFruitSums(token.ClientID)
		if err != nil {
			slog.Error("While flushing fruit", "err", err)
			return
		}
		sum.passToken(token.ClientID, token.TotalTarget, newAccumulated, true, sum.id)
	} else {
		sum.passToken(token.ClientID, token.TotalTarget, newAccumulated, false, -1)
	}
}

func (sum *Sum) flushFruitSums(clientID uint64) error {
	fruitMap, exists := sum.clientFruitItemMap[clientID]
	if exists {
		for key := range fruitMap {
			fruitRecord := []fruititem.FruitItem{fruitMap[key]}
			message, err := inner.SerializeMessage(clientID, fruitRecord)
			if err != nil {
				slog.Debug("While serializing message", "err", err)
				return err
			}
			if err := sum.outputExchange.Send(*message); err != nil {
				slog.Debug("While sending message", "err", err)
				return err
			}
		}
		delete(sum.clientFruitItemMap, clientID)
		delete(sum.clientMessageCounter, clientID)
	}
	return nil
}

func (sum *Sum) passToken(clientID uint64, totalTarget uint64, currentCount uint64, isFinalRound bool, coordinatorID int) {
	tokenMsg, err := inner.SerializeToken(clientID, totalTarget, currentCount, isFinalRound, coordinatorID)
	if err != nil {
		slog.Error("While serializing token", "err", err)
		return
	}
	err = sum.controlOutputQueue.Send(*tokenMsg)
	if err != nil {
		slog.Error("While passing token", "err", err)
		return
	}
	return
}
