package backend

const (
	ClientIDLength = 16

	DEFAULT_QUEUE_IDLE_TIMEOUT      = 60 * 10
	DEFAULT_QUEUE_ABANDON_TIMEOUT   = 60 * 10
	DEFAULT_PRODUCER_BATCH_SIZE     = 20
	DEFAULT_PRODUCER_BATCH_INTERVAL = 5

	DEFAULT_CHANNEL_SIZE       = 100
	DEFAULT_CHANNEL_WORKER_NUM = 1
)

type MessageID string

type Message struct {
	ClientID [ClientIDLength]byte `json:"client_id"`
	ID       MessageID            `json:"id"`
	Body     []byte               `json:"body"`
	pri      int64
	index    int
}

type QueueProducer interface {
	StartBatchProducer() (BatchQueueProducer, error)
	SetTopic(string)
	GetTopic() string
	SendMessage([]byte) error
	CheckActive() bool
	IsActive() bool
	Stop() error
}

/**
* 队列批次发送producer
 */
type BatchQueueProducer interface {
	Topic() string
	SendMessages([][]byte) error
	Stop() error
}
