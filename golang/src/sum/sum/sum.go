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
	inputQueue         middleware.Middleware
	outputExchange     middleware.Middleware
	clientFruitItemMap map[uint64]map[string]fruititem.FruitItem
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

	return &Sum{
		inputQueue:         inputQueue,
		outputExchange:     outputExchange,
		clientFruitItemMap: map[uint64]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
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
		if err := sum.handleEndOfRecordMessage(clientID, totalCount); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(clientID, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientID uint64, totalCount uint64) error {
	slog.Info("Received End Of Records message")

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
	}

	// TODO: not very clean to use this constructor with totalCount=0, maybe add isEof parameter to SerializeMessage
	message, err := inner.SerializeEOF(clientID, 0)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) handleDataMessage(clientID uint64, fruitRecords []fruititem.FruitItem) error {
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
	return nil
}
