package backend

import (
	"sync"

	"github.com/jmuyuyang/ali_mns"
	"github.com/jmuyuyang/queue_proxy/config"
)

const (
	MESSAGE_BUFFER_SIZE = 10
)

type mnsQueue struct {
	pool   sync.Pool
	config config.BackendConfig
	topic  string
	active bool
}

type MnsQueueProducer struct {
	mnsQueue
}

type MnsPipelineProducer struct {
	queue       ali_mns.AliMNSQueue
	dataBuffer  []ali_mns.MessageSendRequest
	messageChan chan ali_mns.MessageSendRequest
	exitChan    chan int
}

func newMnsQueuePool(config config.BackendConfig) sync.Pool {
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

func NewMnsQueueProducer(config config.BackendConfig) *MnsQueueProducer {
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
	}
	_, err := queue.SendMessage(msg)
	return err
}

func (q *MnsQueueProducer) StartPipeline() (PipelineQueueProducer, error) {
	queue := q.pool.Get().(ali_mns.AliMNSQueue)
	queue.SetTopic(q.topic)
	p := &MnsPipelineProducer{
		queue:       queue,
		dataBuffer:  make([]ali_mns.MessageSendRequest, 0),
		messageChan: make(chan ali_mns.MessageSendRequest),
		exitChan:    make(chan int),
	}
	go p.queueLoop()
	return p, nil
}

func (q *MnsPipelineProducer) SendMessage(data []byte) error {
	msg := ali_mns.MessageSendRequest{
		MessageBody: ali_mns.Base64Bytes(data),
	}
	q.messageChan <- msg
	return nil
}

func (q *MnsPipelineProducer) Flush() error {
	return nil
}

func (q *MnsPipelineProducer) Close() error {
	close(q.exitChan)
	return nil
}

/**
* 批量异步发送batch message
 */
func (q *MnsPipelineProducer) queueLoop() {
	var flushDataBuffer bool = false
	for {
		select {
		case msg := <-q.messageChan:
			q.dataBuffer = append(q.dataBuffer, msg)
			if len(q.dataBuffer) >= MESSAGE_BUFFER_SIZE {
				flushDataBuffer = true
			}
		case <-q.exitChan:
			flushDataBuffer = true
			goto exit
		}

		if flushDataBuffer {
			batchMessage := ali_mns.BatchMessageSendRequest{
				Messages: q.dataBuffer,
			}
			q.queue.BatchSendMessage(batchMessage)
			q.dataBuffer = make([]ali_mns.MessageSendRequest, 0)
			flushDataBuffer = false
		}
	}
exit:
	if flushDataBuffer {
		batchMessage := ali_mns.BatchMessageSendRequest{
			Messages: q.dataBuffer,
		}
		q.queue.BatchSendMessage(batchMessage)
		q.dataBuffer = make([]ali_mns.MessageSendRequest, 0)
	}
}
