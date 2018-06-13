package backend

const (
	ClientIDLength = 16
)

type QueueConfig struct {
	Name string                 `yaml:"name"`
	Type string                 `yaml:"type"`
	Attr map[string]interface{} `yaml:"attr"`
}

type MessageID string

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
	Stop() error
}
