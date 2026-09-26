package inner

import (
	"encoding/json"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageBody struct {
	ClientID     uint64                `json:"client_id"`
	IsEOF        bool                  `json:"is_eof"`
	FruitRecords []fruititem.FruitItem `json:"fruit_records"`
	TotalCount   int64                 `json:"total_count"`
}

func NewMessageBody(clientID uint64, fruitRecords []fruititem.FruitItem, isEOF bool) (*middleware.Message, error) {
	if fruitRecords == nil {
		fruitRecords = []fruititem.FruitItem{}
	}
	body := &MessageBody{
		ClientID:     clientID,
		IsEOF:        isEOF,
		FruitRecords: fruitRecords,
		TotalCount:   0,
	}
	jsonBytes, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(jsonBytes)}, nil
}

func Deserialize(msg *middleware.Message) (*MessageBody, error) {
	body := &MessageBody{}
	err := json.Unmarshal([]byte(msg.Body), body)
	if err != nil {
		return nil, err
	}
	return body, nil
}
