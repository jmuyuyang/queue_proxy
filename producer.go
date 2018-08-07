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
	Stop() error
}

type QueueProducerObject struct {
	sync.RWMutex
	Name           string
	config         config.Config
	queue          QueueProducer
	diskQueue      *backend.DiskQueue
	rateController *rateio.Controller
	checkQueueChan chan int
	exitChan       chan int
	pauseChan      chan bool
	logFunc        util.LoggerFuncHandler
}

/*
* 解析配置文件
 */
func ParseConfigFile(cfgFile string) (config.Config, error) {
	return config.ParseConfigFile(cfgFile)
}

/**
* 消息服务producer object
 */
func NewQueueProducer(config config.Config) *QueueProducerObject {
	senderObj := &QueueProducerObject{
		config:         config,
		checkQueueChan: make(chan int, CHECK_QUEUE_CHAIN_BUFFER),
		exitChan:       make(chan int),
		logFunc:        func(level util.LogLevel, message string) {},
	}
	return senderObj
}

/**
* 直接设置queue object
 */
func (q *QueueProducerObject) SetQueue(queue QueueProducer) {
	q.queue = queue
}

/**
* 初始化queue producer
 */
func (q *QueueProducerObject) InitQueue(name string, topicName string, queueTypeName string) {
	q.Name = name
	if q.config.DiskConfig.Path != "" {
		diskQueue, err := createDiskQueue(name, q.config.DiskConfig)
		if err == nil {
			q.diskQueue = diskQueue
		}
	}
	if queueTypeName != "" {
		q.setQueueAttr(queueTypeName, topicName)
	}
}

/*
* 设置队列相关属性
 */
func (q *QueueProducerObject) setQueueAttr(queueTypeName string, topicName string) {
	if q.queue == nil {
		q.queue = createQueueProducer(q.config.GetQueueConfig(queueTypeName))
		q.queue.SetTopic(topicName)
	}
}

/**
* 设置queue topic
 */
func (q *QueueProducerObject) SetTopic(topicName string) {
	if q.queue != nil {
		q.Lock()
		defer q.Unlock()
		q.doPause(true)
		q.queue.SetTopic(topicName)
		q.doPause(false)
	}
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
	if q.queue == nil {
		q.logFunc(util.InfoLvl, "cannot find queue source")
	}
	if q.diskQueue != nil {
		go q.startBackendLoop()
	}
}

/**
* 停止队列sender运行
 */
func (q *QueueProducerObject) Stop() {
	q.Lock()
	defer q.Unlock()
	close(q.exitChan)
	q.diskQueue.Stop()
	q.queue.Stop()
}

/**
* disk queue暂停/重启
 */
func (q *QueueProducerObject) doPause(pause bool) {
	if q.diskQueue != nil {
		select {
		case q.pauseChan <- pause:
		default:
			//如果pauseChan为nil,则拥有阻塞
		}
	}
}

/**
* 设置限速
 */
func (q *QueueProducerObject) SetRateLimit(ratePerSecond int) {
	q.doPause(true)
	if ratePerSecond == 0 {
		return
	}
	if q.rateController == nil {
		q.Lock()
		q.rateController = rateio.NewController(ratePerSecond)
		q.rateController.Start()
		q.Unlock()
	} else {
		q.rateController.SetRateLimit(ratePerSecond)
	}
	q.doPause(false)
}

/**
* 关闭限速
 */
func (q *QueueProducerObject) DisableRateLimit() {
	if q.rateController != nil {
		q.Lock()
		defer q.Unlock()
		q.doPause(true)
		q.rateController.Stop()
		q.rateController = nil
		q.doPause(false)
	}
}

/**
* 发送消息
 */
func (q *QueueProducerObject) SendMessage(data []byte, async bool) error {
	var addBackendStore bool = false
	var err error
	if async {
		addBackendStore = true
	} else {
		q.RLock()
		addBackendStore = false
		if q.queue != nil && q.queue.IsActive() {
			if q.rateController != nil && !q.rateController.Assign(false) {
				//超过限速
				addBackendStore = true
			} else {
				err = q.queue.SendMessage(data)
				if err != nil {
					//触发队列检测
					q.logFunc(util.ErrorLvl, "add message error:"+err.Error())
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

/**
* 启动 backend disk queue loop
 */
func (q *QueueProducerObject) startBackendLoop() {
	checkQueueTicker := time.NewTicker(CHECK_QUEUE_TIMEOUT) //监测队列链接是否正常
	var pipelineQueue backend.PipelineQueueProducer
	var pause bool = false
	var r chan []byte
	var err error
	q.pauseChan = make(chan bool)
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
					q.logFunc(util.ErrorLvl, "backend flush message error:"+err.Error())
					q.withRecover(func() {
						pipelineQueue.Stop()
					})
					pipelineQueue = nil
				}
			}
		case <-checkQueueTicker.C:
			if pause {
				continue
			}
			if pipelineQueue != nil {
				//每次定期队列检测，强制关闭一次pipeline queue
				q.withRecover(func() {
					pipelineQueue.Stop()
				})
				pipelineQueue = nil
			}
			if q.queue != nil {
				if q.queue.CheckActive() {
					q.logFunc(util.DebugLvl, "checked connected successed")
					if r == nil {
						r = q.diskQueue.GetMessageChan()
					}
				} else {
					q.logFunc(util.InfoLvl, "checked connected failed")
					r = nil
				}
			}
		case <-q.checkQueueChan:
			if pause {
				continue
			}
			if q.queue != nil && q.queue.IsActive() {
				//主动发起的队列检测，仅当queue is active时才触发
				if q.queue.CheckActive() {
					q.logFunc(util.DebugLvl, "checked connected successed")
					if r == nil {
						r = q.diskQueue.GetMessageChan()
					}
				} else {
					q.logFunc(util.InfoLvl, "checked connected failed")
					if pipelineQueue != nil {
						pipelineQueue = nil
					}
					r = nil
				}
			}
		case pause = <-q.pauseChan:
			if pipelineQueue != nil {
				q.withRecover(func() {
					pipelineQueue.Stop()
				})
				pipelineQueue = nil
			}
			if pause {
				//禁止从 diskQueue 读数据
				r = nil
			}
		case <-q.exitChan:
			if pipelineQueue != nil {
				q.withRecover(func() {
					pipelineQueue.Stop()
				})
			}
			goto exit
		}
	}
exit:
	checkQueueTicker.Stop()
}

func (q *QueueProducerObject) withRecover(handler func()) {
	defer func() {
		if err := recover(); err != nil {
			if _, ok := err.(error); ok {
				q.logFunc(util.ErrorLvl, err.(error).Error())
			}
		}
	}()
	handler()
}

func createDiskQueue(topicName string, config config.DiskConfig) (*backend.DiskQueue, error) {
	diskQueue := backend.NewDiskQueue(config)
	diskQueue.SetTopic(topicName)
	err := diskQueue.Start()
	if err != nil {
		return nil, err
	}
	return diskQueue, nil
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
