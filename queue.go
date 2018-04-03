package queue

import (
	"fmt"
	"sync"
	"time"

	"github.com/jmuyuyang/queue_proxy/backend"
	"github.com/jmuyuyang/queue_proxy/config"
	"github.com/jmuyuyang/queue_proxy/rateio"
	"github.com/jmuyuyang/queue_proxy/util"
)

const CHECK_QUEUE_TIMEOUT = 5 * time.Second
const CHECK_QUEUE_CHAIN_BUFFER = 3

type QueueProducer interface {
	StartPipeline() (backend.PipelineQueueProducer, error)
	SetTopic(string)
	GetTopic() string
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

type QueueProducerObject struct {
	sync.RWMutex
	config         config.Config
	queue          QueueProducer
	diskQueue      *backend.DiskQueue
	rateController *rateio.Controller
	checkQueueChan chan int
	exitChan       chan int
	pauseChan      chan bool
	logFunc        util.LoggerFuncHandler
}

type QueueConsumerObject struct {
	config config.Config
	queue  QueueConsumer
}

/*
* 解析配置文件
 */
func ParseConfigFile(cfgFile string) (config.Config, error) {
	return config.ParseConfigFile(cfgFile)
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

func (t *QueueConsumerObject) SetQueueAttr(queueTypeName string, topicName string, options *backend.Options) {
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

/**
* 消息服务producer object
 */
func NewQueueProducer(config config.Config) *QueueProducerObject {
	senderObj := &QueueProducerObject{
		config:         config,
		checkQueueChan: make(chan int, CHECK_QUEUE_CHAIN_BUFFER),
		exitChan:       make(chan int),
		pauseChan:      make(chan bool),
		logFunc:        func(level util.LogLevel, message string) {},
	}
	return senderObj
}

/*
* 设置队列相关属性
 */
func (q *QueueProducerObject) SetQueueAttr(queueTypeName string, topicName string) {
	if q.config.DiskConfig.Path != "" {
		diskQueue, err := createDiskQueue(topicName, q.config.DiskConfig)
		if err == nil {
			q.diskQueue = diskQueue
		}
	}
	q.SetQueueTypeName(queueTypeName)
	q.SetTopic(topicName)
}

/**
* 设置队列类型
 */
func (q *QueueProducerObject) SetQueueTypeName(queueTypeName string) {
	q.Lock()
	defer q.Unlock()
	q.doPause(true)
	q.queue = createQueueProducer(q.config.GetQueueConfig(queueTypeName))
	q.doPause(false)
}

func (q *QueueProducerObject) SetLogger(logger util.LoggerFuncHandler) {
	topic := q.GetTopic()
	q.logFunc = func(level util.LogLevel, message string) {
		msg := fmt.Sprintf("backend queue topic:%s;%s", topic, message)
		logger(level, msg)
	}
	if q.diskQueue != nil {
		q.diskQueue.SetLogger(q.logFunc)
	}
}

/**
* 设置queue topic
 */
func (q *QueueProducerObject) SetTopic(topicName string) {
	if q.queue != nil {
		q.queue.SetTopic(topicName)
	}
}

/**
* 获取queue topic
 */
func (q *QueueProducerObject) GetTopic() string {
	if q.queue != nil {
		return q.queue.GetTopic()
	}
	return ""
}

/**
* disk queue 启动
 */
func (q *QueueProducerObject) Start() {
	go q.startBackend()
}

/**
* 停止队列sender运行
 */
func (q *QueueProducerObject) Stop() {
	q.Lock()
	defer q.Unlock()
	close(q.exitChan)
	q.diskQueue.Stop()
}

/**
* disk queue暂停/重启
 */
func (q *QueueProducerObject) doPause(pause bool) {
	if q.diskQueue != nil {
		select {
		case q.pauseChan <- pause:
		default:
		}
	}
}

/**
* 设置限速
 */
func (q *QueueProducerObject) SetRateLimit(ratePerSecond int) {
	if q.rateController == nil {
		q.Lock()
		defer q.Unlock()
		q.doPause(true)
		q.rateController = rateio.NewController(ratePerSecond)
		q.rateController.Start()
		q.doPause(false)
	} else {
		q.rateController.SetRateLimit(ratePerSecond)
	}
}

/**
* 关闭限速
 */
func (q *QueueProducerObject) DisableRateLimit() {
	q.Lock()
	defer q.Unlock()
	q.doPause(true)
	q.rateController.Stop()
	q.rateController = nil
	q.doPause(false)
}

/**
* 发送消息
 */
func (q *QueueProducerObject) SendMessage(data []byte, async bool) error {
	var addBackendStore bool = false
	if async {
		addBackendStore = true
	} else {
		q.RLock()
		addBackendStore = false
		var err error
		if q.queue != nil && q.queue.IsActive() {
			if q.rateController != nil && !q.rateController.Assign(false) {
				//超过限速
				addBackendStore = true
			} else {
				err = q.queue.SendMessage(data)
				if err != nil {
					//触发队列检测
					q.logFunc(util.InfoLvl, "add backend queue error:"+err.Error())
					q.checkQueueChan <- 1
					addBackendStore = true
				}
			}
		} else {
			addBackendStore = true
		}
		q.RUnlock()
	}
	if addBackendStore {
		//添加到灾备磁盘队列
		if q.diskQueue != nil {
			q.diskQueue.SendMessage(data)
		}
	}
	return err
}

func (q *QueueProducerObject) startBackend() {
	if q.diskQueue == nil {
		return
	}
	checkQueueTicker := time.NewTicker(CHECK_QUEUE_TIMEOUT) //监测队列链接是否正常
	var pipelineQueue backend.PipelineQueueProducer
	var r chan []byte
	if q.queue != nil {
		r = q.diskQueue.GetMessageChan()
	}
	var err error
	for {
		select {
		case dataByte := <-r:
			if pipelineQueue == nil {
				pipelineQueue, err = q.queue.StartPipeline()
			}
			if err != nil {
				//创建pipeline队列失败,则直接禁止读取disk queue
				q.diskQueue.SendMessage(dataByte)
				pipelineQueue = nil
				r = nil
			}
			if pipelineQueue != nil {
				if q.rateController != nil {
					//等待限速
					q.rateController.Assign(true)
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
			if q.queue != nil && q.queue.CheckActive() {
				q.logFunc(util.DebugLvl, "checked connected successed")
				r = q.diskQueue.GetMessageChan()
			} else {
				q.logFunc(util.InfoLvl, "checked connected failed")
				r = nil
			}
		case pause := <-q.pauseChan:
			if pipelineQueue != nil {
				pipelineQueue.Close()
				pipelineQueue = nil
			}
			if pause {
				//禁止从 diskQueue 读数据
				r = nil
			}
		case <-q.checkQueueChan:
			if q.queue == nil || q.queue.IsActive() && !q.queue.CheckActive() {
				q.logFunc(util.InfoLvl, "checked connected failed")
				if pipelineQueue != nil {
					pipelineQueue.Close()
					pipelineQueue = nil
				}
				r = nil
			}
		case <-q.exitChan:
			if pipelineQueue != nil {
				pipelineQueue.Close()
			}
			goto exit
		}
	}
exit:
	checkQueueTicker.Stop()
}

func NewConsumerOptions() *backend.Options {
	return backend.NewOptions()
}

func createDiskQueue(topicName string, config config.DiskConfig) (*backend.DiskQueue, error) {
	diskQueue, err := backend.NewDiskQueue(config)
	if err != nil {
		return nil, err
	}
	diskQueue.SetTopic(config.Prefix + "-" + topicName)
	err = diskQueue.Start()
	return diskQueue, err
}

func createQueueProducer(cfg config.QueueConfig) QueueProducer {
	if cfg.Type == config.TYPE_REDIS {
		return backend.NewRedisQueueProducer(cfg.Attr)
	}
	if cfg.Type == config.TYPE_KAFKA {
		return backend.NewKafkaQueueProducer(cfg.Attr)
	}
	if cfg.Type == config.TYPE_MNS {
		return backend.NewMnsQueueProducer(cfg.Attr)
	}
	return nil
}

func createQueueConsumer(cfg config.QueueConfig, options *backend.Options) QueueConsumer {
	if cfg.Type == config.TYPE_REDIS {
		return backend.NewRedisQueueConsumer(cfg.Attr, options)
	}
	return nil
}
