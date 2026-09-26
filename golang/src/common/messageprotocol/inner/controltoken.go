package inner

import (
	"encoding/json"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type ControlToken struct {
	ClientID      uint64 `json:"client_id"`
	TotalTarget   uint64 `json:"total_target"`
	CurrentCount  uint64 `json:"current_count"`
	IsFinalRound  bool   `json:"is_final_round"`
	CoordinatorID int    `json:"coordinator_id"`
}

func SerializeToken(clientID uint64, totalTarget uint64, currentCount uint64, isFinalRound bool, coordinatorID int) (*middleware.Message, error) {
	token := &ControlToken{
		ClientID:      clientID,
		TotalTarget:   totalTarget,
		CurrentCount:  currentCount,
		IsFinalRound:  isFinalRound,
		CoordinatorID: coordinatorID,
	}
	jsonBytes, err := json.Marshal(token)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(jsonBytes)}, nil
}

func DeserializeToken(msg *middleware.Message) (*ControlToken, error) {
	token := &ControlToken{}
	err := json.Unmarshal([]byte(msg.Body), token)
	if err != nil {
		return nil, err
	}
	return token, nil
}
