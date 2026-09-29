package inner

type ControlFlag string

const (
	COORD       ControlFlag = "coordinator"
	COUNT       ControlFlag = "count"
	FINAL_ROUND ControlFlag = "final_round"
)

type ControlToken struct {
	ClientID      uint64      `json:"client_id"`
	CurrentCount  uint64      `json:"current_count"`
	Flag          ControlFlag `json:"flag"`
	CoordinatorID int         `json:"coordinator_id"`
}
