package backend

const (
	MsgIDLength = 16
)

type MessageID [MsgIDLength]byte

type Message struct {
	ID   MessageID
	Body []byte
}

type PipelineQueueProducer interface {
	SendMessage([]byte) error
	Flush() error
	Close() error
}
