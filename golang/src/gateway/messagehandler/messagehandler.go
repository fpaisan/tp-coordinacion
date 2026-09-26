package messagehandler

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var nextClientID uint64 = 0

type MessageHandler struct {
	clientID     uint64
	messageCount uint64
}

func NewMessageHandler() MessageHandler {
	clientID := nextClientID
	nextClientID++
	return MessageHandler{clientID: clientID, messageCount: 0}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	messageHandler.messageCount++
	return inner.SerializeMessage(messageHandler.clientID, data, false)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeMessage(messageHandler.clientID, nil, true)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	clientID, fruitRecords, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	if clientID != messageHandler.clientID {
		return nil, nil
	}
	return fruitRecords, nil
}
