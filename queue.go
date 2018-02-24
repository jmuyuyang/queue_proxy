package queue

import (
	"time"

	"github.com/jmuyuyang/queue_proxy/backend"
	"github.com/jmuyuyang/queue_proxy/rateio"
)

const CHECK_QUEUE_TIMEOUT = 5 * time.Second
const CHECK_QUEUE_CHAIN_BUFFER = 3

type QueueProducer interface {
	StartPipeline() (backend.PipelineQueueProducer, error)
	SetTopic(string)
	SendMessage([]byte) error
	CheckActive() bool
	IsActive() bool
}

type QueueConsumer interface {
	Start()
	Stop()
	SetTopic(string)
	GetOpts() *backend.Options
	GetMessageChan() chan *backend.Message
	AckMessage(backend.MessageID) error
}

type QueueConfig struct {
	Type  string              `yaml:"type"`
	Redis backend.RedisConfig `yaml:"redis"`
	Kafka backend.KafkaConfig `yaml:"kafka"`
	Disk  backend.DiskConfig  `yaml:"disk"`
}

type QueueProducerObject struct {
	config         QueueConfig
	queue          QueueProducer
	diskQueue      *backend.DiskQueue
	rateController *rateio.Controller
	checkQueueChan chan int
	exitChan       chan int
}

type QueueConsumerObject struct {
	topic  string
	config QueueConfig
	queue  QueueConsumer
}

/**
* 消息服务consumer object
 */
func NewQueueConsumer(topicName string, config QueueConfig, options *backend.Options) *QueueConsumerObject {
	if options == nil {
		options = backend.NewOptions()
	}
	consumerObj := &QueueConsumerObject{
		topic:  topicName,
		config: config,
		queue:  createQueueConsumer(config, options),
	}
	consumerObj.SetTopic(topicName)
	return consumerObj
}

/**
* 设置队列类型
 */
func (t *QueueConsumerObject) SetQueueType(queueType string) {
	t.config.Type = queueType
	t.queue = createQueueConsumer(t.config, backend.NewOptions())
}

/**
* 设置publish 主题
 */
func (t *QueueConsumerObject) SetTopic(topicName string) {
	t.topic = topicName
	t.queue.SetTopic(topicName)
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

/**
* 消息服务producer object
 */
func NewQueueProducer(topicName string, config QueueConfig) *QueueProducerObject {
	senderObj := &QueueProducerObject{
		config:         config,
		queue:          createQueueProducer(config),
		checkQueueChan: make(chan int, CHECK_QUEUE_CHAIN_BUFFER),
		exitChan:       make(chan int),
	}
	if config.Disk.Path != "" {
		diskQueue, err := createDiskQueue(topicName, senderObj.config.Disk)
		if err == nil {
			senderObj.diskQueue = diskQueue
		}
	}
	senderObj.SetTopic(topicName)
	return senderObj
}

/**
* 设置队列类型
 */
func (t *QueueProducerObject) SetQueueType(queueType string) {
	t.config.Type = queueType
	t.queue = createQueueProducer(t.config)
}

/**
* 设置publish 主题
 */
func (t *QueueProducerObject) SetTopic(topicName string) {
	t.queue.SetTopic(topicName)
}

/**
* disk queue 启动
 */
func (t *QueueProducerObject) Start() {
	go t.startBackend()
}

/**
* 设置限速
 */
func (t *QueueProducerObject) SetRateLimit(ratePerSecond int) {
	if t.rateController == nil {
		t.rateController = rateio.NewController(ratePerSecond)
	} else {
		t.rateController.SetRateLimit(ratePerSecond)
	}
}

/**
* 关闭限速
 */
func (t *QueueProducerObject) DisableRateLimit() {
	t.rateController.Close()
}

/**
* 发送消息
 */
func (sd *QueueProducerObject) SendMessage(data []byte) error {
	addBackendStore := false
	var err error
	if sd.queue != nil && sd.queue.IsActive() {
		if sd.rateController != nil && !sd.rateController.Assign(false) {
			//超过限速
			addBackendStore = true
		} else {
			err = sd.queue.SendMessage(data)
			if err != nil {
				//触发队列检测
				sd.checkQueueChan <- 1
				addBackendStore = true
			}
		}
	} else {
		addBackendStore = true
	}
	if addBackendStore {
		//添加到灾备磁盘队列
		if sd.diskQueue != nil {
			sd.diskQueue.SendMessage(data)
		}
	}
	return err
}

func (sd *QueueProducerObject) startBackend() {
	if sd.diskQueue == nil {
		return
	}
	checkQueueTicker := time.NewTicker(CHECK_QUEUE_TIMEOUT) //监测队列链接是否正常
	r := sd.diskQueue.GetMessageChan()
	var pipelineQueue backend.PipelineQueueProducer
	var err error
	for {
		select {
		case dataByte := <-r:
			if pipelineQueue == nil {
				pipelineQueue, err = sd.queue.StartPipeline()
			}
			if err != nil {
				//创建pipeline队列失败,则直接禁止读取disk queue
				sd.diskQueue.SendMessage(dataByte)
				pipelineQueue = nil
				r = nil
			}
			if pipelineQueue != nil {
				if sd.rateController != nil {
					//等待限速
					sd.rateController.Assign(true)
				}
				err := pipelineQueue.SendMessage(dataByte)
				if err != nil {
					pipelineQueue.Close()
					pipelineQueue = nil
				}
			}
		case <-checkQueueTicker.C:
			if pipelineQueue != nil {
				pipelineQueue.Close()
				pipelineQueue = nil
			}
			if sd.queue != nil && sd.queue.CheckActive() {
				r = sd.diskQueue.GetMessageChan()
			} else {
				r = nil
			}
		case <-sd.checkQueueChan:
			if sd.queue == nil || sd.queue.IsActive() && !sd.queue.CheckActive() {
				if pipelineQueue != nil {
					pipelineQueue.Close()
					pipelineQueue = nil
				}
				r = nil
			}
		case <-sd.exitChan:
			if pipelineQueue != nil {
				pipelineQueue.Close()
			}
			goto exit
		}
	}
exit:
}

/**
* 停止队列sender运行
 */
func (sd *QueueProducerObject) Stop() {
	close(sd.exitChan)
	sd.diskQueue.Stop()
}

func NewConsumerOptions() *backend.Options {
	return backend.NewOptions()
}

func createDiskQueue(topicName string, config backend.DiskConfig) (*backend.DiskQueue, error) {
	diskQueue, err := backend.NewDiskQueue(config)
	if err != nil {
		return nil, err
	}
	diskQueue.SetTopic(config.Prefix + "-" + topicName)
	err = diskQueue.Start()
	return diskQueue, err
}

func createQueueProducer(config QueueConfig) QueueProducer {
	if config.Type == "redis" {
		return backend.NewRedisQueueProducer(config.Redis)
	}
	if config.Type == "kafka" {
		return backend.NewKafkaQueueProducer(config.Kafka)
	}
	return nil
}

func createQueueConsumer(config QueueConfig, options *backend.Options) QueueConsumer {
	if config.Type == "redis" {
		return backend.NewRedisQueueConsumer(config.Redis, options)
	}
	return nil
}
