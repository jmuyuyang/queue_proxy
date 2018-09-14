package backend

import (
	"sync"

	"github.com/jmuyuyang/ali_mns"
	"github.com/jmuyuyang/queue_proxy/config"
)

type mnsQueue struct {
	pool   sync.Pool
	config config.QueueAttrConfig
	topic  string
	active bool
}

type MnsQueueProducer struct {
	mnsQueue
}

type MnsAsyncProducer struct {
	queue ali_mns.AliMNSQueue
	topic string
}

func newMnsQueuePool(config config.QueueAttrConfig) sync.Pool {
	accessKey := config.Attr["access_key"].(string)
	accessKeySecret := config.Attr["access_secret"].(string)
	return sync.Pool{
		New: func() interface{} {
			client := ali_mns.NewAliMNSClient(config.Bind, accessKey, accessKeySecret)
			client.SetTimeout(config.Timeout)
			queue := ali_mns.NewMNSQueue(client)
			return queue
		},
	}
}

func (q *mnsQueue) SetTopic(topic string) {
	q.topic = topic
}

func (q *mnsQueue) GetTopic() string {
	return q.topic
}

func (q *mnsQueue) IsActive() bool {
	return q.active
}

func (q *mnsQueue) CheckActive() bool {
	queue := q.pool.Get().(ali_mns.AliMNSQueue)
	defer q.pool.Put(queue)
	cli := queue.GetClient()
	queueManager := ali_mns.NewMNSQueueManager(cli)
	_, err := queueManager.GetQueueAttributes(q.topic)
	if err != nil {
		//无法获取队列属性
		q.active = false
		return false
	}
	q.active = true
	return true
}

func NewMnsQueueProducer(config config.QueueAttrConfig) *MnsQueueProducer {
	return &MnsQueueProducer{
		mnsQueue: mnsQueue{
			pool:   newMnsQueuePool(config),
			config: config,
			active: true,
		},
	}
}

/**
* 发送mns message request
 */
func (q *MnsQueueProducer) SendMessage(data []byte) error {
	queue := q.pool.Get().(ali_mns.AliMNSQueue)
	defer q.pool.Put(queue)
	queue.SetTopic(q.topic)
	msg := ali_mns.MessageSendRequest{
		MessageBody: ali_mns.Base64Bytes(data),
		Priority:    1,
	}
	_, err := queue.SendMessage(msg)
	return err
}

/**
* 停止mns queue producer
 */
func (q *MnsQueueProducer) Stop() error {
	return nil
}

func (q *MnsQueueProducer) StartBatchProducer() (BatchQueueProducer, error) {
	queue := q.pool.Get().(ali_mns.AliMNSQueue)
	queue.SetTopic(q.topic)
	p := &MnsAsyncProducer{
		queue: queue,
		topic: q.topic,
	}
	return p, nil
}

func (q *MnsAsyncProducer) Topic() string {
	return q.topic
}

/**
* 批量发送mns消息
 */
func (q *MnsAsyncProducer) SendMessages(items [][]byte) error {
	messageList := make([]ali_mns.MessageSendRequest, 0)
	for _, item := range items {
		msg := ali_mns.MessageSendRequest{
			MessageBody: ali_mns.Base64Bytes(item),
			Priority:    1,
		}
		messageList = append(messageList, msg)
	}
	batchMessage := ali_mns.BatchMessageSendRequest{
		Messages: messageList,
	}
	_, err := q.queue.BatchSendMessage(batchMessage)
	return err
}

func (q *MnsAsyncProducer) Stop() error {
	return nil
}
