package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue        middleware.Middleware
	inputExchange      middleware.Middleware
	clientFruitItemMap map[uint64]map[string]fruititem.FruitItem
	topSize            int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:        outputQueue,
		inputExchange:      inputExchange,
		clientFruitItemMap: map[uint64]map[string]fruititem.FruitItem{},
		topSize:            config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientID, fruitRecords, isEof, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := aggregation.handleEndOfRecordsMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	aggregation.handleDataMessage(clientID, fruitRecords)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID uint64) error {
	slog.Info("Received End Of Records message")

	fruitTopRecords := aggregation.buildFruitTop(clientID)
	message, err := inner.SerializeMessage(clientID, fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	message, err = inner.SerializeEOF(clientID, 0)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	delete(aggregation.clientFruitItemMap, clientID)
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientID uint64, fruitRecords []fruititem.FruitItem) {
	if _, exists := aggregation.clientFruitItemMap[clientID]; !exists {
		aggregation.clientFruitItemMap[clientID] = map[string]fruititem.FruitItem{}
	}
	currentClientMap := aggregation.clientFruitItemMap[clientID]
	for _, fruitRecord := range fruitRecords {
		if _, ok := currentClientMap[fruitRecord.Fruit]; ok {
			currentClientMap[fruitRecord.Fruit] = currentClientMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			currentClientMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientID uint64) []fruititem.FruitItem {
	currentClientMap := aggregation.clientFruitItemMap[clientID]
	fruitItems := make([]fruititem.FruitItem, 0, len(currentClientMap))
	for _, item := range currentClientMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
