package queue

import (
	"github.com/jmuyuyang/queue_proxy/backend"
	"github.com/jmuyuyang/queue_proxy/config"
)

type QueueConsumer interface {
	Start()
	Stop()
	SetTopic(string)
	GetOpts() *backend.Options
	GetMessageChan() chan *backend.Message
	AckMessage(backend.MessageID) error
}

type QueueConsumerObject struct {
	config config.Config
	queue  QueueConsumer
}

func NewConsumerOptions() *backend.Options {
	return backend.NewOptions()
}

/**
* 消息服务consumer object
 */
func NewQueueConsumer(config config.Config) *QueueConsumerObject {
	consumerObj := &QueueConsumerObject{
		config: config,
	}
	return consumerObj
}

func (t *QueueConsumerObject) InitQueue(queueTypeName string, topicName string, options *backend.Options) {
	if options == nil {
		options = backend.NewOptions()
	}
	t.SetQueueTypeName(queueTypeName, options)
	t.SetTopic(topicName)
}

/**
* 设置队列类型
 */
func (t *QueueConsumerObject) SetQueueTypeName(queueTypeName string, options *backend.Options) {
	t.queue = createQueueConsumer(t.config.GetQueueConfig(queueTypeName), options)
}

/**
* 设置publish 主题
 */
func (t *QueueConsumerObject) SetTopic(topicName string) {
	if t.queue != nil {
		t.queue.SetTopic(topicName)
	}
}

func (t *QueueConsumerObject) Start() {
	t.queue.Start()
}

func (t *QueueConsumerObject) Stop() {
	t.queue.Stop()
}

func (t *QueueConsumerObject) GetOpts() *backend.Options {
	return t.queue.GetOpts()
}

/*
* 获取消息消费channel
 */
func (t *QueueConsumerObject) GetMessageChan() chan *backend.Message {
	return t.queue.GetMessageChan()
}

/**
* ack queue message
 */
func (t *QueueConsumerObject) AckMessage(msgId backend.MessageID) error {
	return t.queue.AckMessage(msgId)
}

func createQueueConsumer(cfg config.QueueConfig, options *backend.Options) QueueConsumer {
	if cfg.Type == config.TYPE_REDIS {
		return backend.NewRedisQueueConsumer(cfg.Attr, options)
	}
	return nil
}
