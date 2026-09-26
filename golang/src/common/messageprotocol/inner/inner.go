package inner

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func SerializeMessage(clientID uint64, fruitRecords []fruititem.FruitItem, isEOF bool) (*middleware.Message, error) {
	return NewMessageBody(clientID, fruitRecords, isEOF)
}

func DeserializeMessage(message *middleware.Message) (uint64, []fruititem.FruitItem, bool, error) {
	body, err := Deserialize(message)
	if err != nil {
		return 0, nil, false, err
	}
	return body.ClientID, body.FruitRecords, body.IsEOF, nil
}
