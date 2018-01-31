package backend

const (
	MsgIDLength    = 16
	ClientIDLength = 16
)

type MessageID [MsgIDLength]byte

type Message struct {
	ClientID [ClientIDLength]byte `json:"client_id"`
	ID       MessageID            `json:"id"`
	Body     []byte               `json:"body"`
	pri      int64
	index    int
}

type PipelineQueueProducer interface {
	SendMessage([]byte) error
	Flush() error
	Close() error
}