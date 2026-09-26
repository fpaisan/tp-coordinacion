package inner

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func SerializeMessage(clientID uint64, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	return NewMessageBody(clientID, fruitRecords)
}

func SerializeEOF(clientID uint64, totalCount uint64) (*middleware.Message, error) {
	return NewEOFMessageBody(clientID, totalCount)
}

func DeserializeMessage(message *middleware.Message) (uint64, []fruititem.FruitItem, bool, uint64, error) {
	body, err := Deserialize(message)
	if err != nil {
		return 0, nil, false, 0, err
	}
	return body.ClientID, body.FruitRecords, body.IsEof, body.TotalCount, err
}
