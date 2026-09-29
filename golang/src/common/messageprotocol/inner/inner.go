package inner

import (
	"encoding/json"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func SerializeMessage(clientID uint64, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	return NewMessageBody(clientID, fruitRecords)
}

func SerializeEOF(clientID uint64, totalCount uint64) (*middleware.Message, error) {
	return NewEOFMessageBody(clientID, totalCount)
}

func SerializeControlToken(token *ControlToken) (*middleware.Message, error) {
	jsonBytes, err := json.Marshal(token)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(jsonBytes)}, nil
}

func DeserializeMessage(message *middleware.Message) (uint64, []fruititem.FruitItem, bool, uint64, error) {
	body, err := Deserialize(message)
	if err != nil {
		return 0, nil, false, 0, err
	}
	return body.ClientID, body.FruitRecords, body.IsEof, body.TotalCount, err
}

func DeserializeControlToken(message *middleware.Message) (*ControlToken, error) {
	token := &ControlToken{}
	if err := json.Unmarshal([]byte(message.Body), &token); err != nil {
		return token, err
	}
	return token, nil
}
