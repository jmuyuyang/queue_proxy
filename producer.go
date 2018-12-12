package queue

import (
	"fmt"
	"sync"
	"time"

	"github.com/jmuyuyang/queue_proxy/backend"
	"github.com/jmuyuyang/queue_proxy/channel"
	"github.com/jmuyuyang/queue_proxy/config"
	"github.com/jmuyuyang/queue_proxy/queue"
	"github.com/jmuyuyang/queue_proxy/util"
)

const CHECK_QUEUE_TIMEOUT = 5 * time.Second
const CHECK_QUEUE_CHAIN_BUFFER = 3

type QueueProducerObject struct {
	sync.RWMutex
	Name            string
	config          config.Config
	queue           backend.QueueProducer
	diskQueue       *queue.DiskQueue
	dataChannel     *channel.Channel
	rateLimitEnable bool
	checkQueueChan  chan int
	exitChan        chan int
	pauseChan       chan bool
	logFunc         util.LoggerFuncHandler
	waitGroup       util.WaitGroupWrapper
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
		config:          config,
		checkQueueChan:  make(chan int, CHECK_QUEUE_CHAIN_BUFFER),
		exitChan:        make(chan int),
		logFunc:         func(level util.LogLevel, message string) {},
		rateLimitEnable: false,
	}
	return senderObj
}

/**
* 初始化queue producer
 */
func (q *QueueProducerObject) InitQueue(name string, topicName string, queueTypeName string) error {
	if q.config.DiskConfig.Path == "" {
		return fmt.Errorf("disk queue path must be provide")
	}
	q.Name = name
	err := q.setQueueAttr(queueTypeName, topicName)
	if err != nil {
		return err
	}
	q.initDiskQueue(name, q.config.DiskConfig)
	q.initDataChannel()
	return nil
}

/*
* 设置队列相关属性
 */
func (q *QueueProducerObject) setQueueAttr(queueTypeName string, topicName string) error {
	if q.queue == nil {
		queueCfg := q.config.GetQueueConfig(queueTypeName)
		if queueCfg.Name == "" {
			return fmt.Errorf("cannot find queue config: %s", queueTypeName)
		}
		q.queue = createQueueProducer(queueCfg)
		q.queue.SetTopic(topicName)
	}
	return nil
}

/**
* 设置日志记录器
 */
func (q *QueueProducerObject) SetLogger(logger util.LoggerFuncHandler) {
	topic := q.GetTopic()
	q.logFunc = func(level util.LogLevel, message string) {
		msg := fmt.Sprintf("backend queue topic:%s;%s", topic, message)
		logger(level, msg)
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
func (q *QueueProducerObject) Start() error {
	if q.queue == nil || q.diskQueue == nil {
		return fmt.Errorf("cannot find queue source")
	}
	err := q.diskQueue.Start()
	if err != nil {
		return err
	}
	q.waitGroup.Wrap(q.startBackendLoop)
	return nil
}

/**
* 停止队列sender运行
 */
func (q *QueueProducerObject) Stop() {
	q.Lock()
	defer q.Unlock()
	close(q.exitChan)
	q.waitGroup.Wait()
	q.queue.Stop()
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
			//如果pauseChan为nil,则拥有阻塞
		}
	}
}

/**
* 设置限速
 */
func (q *QueueProducerObject) SetRateLimit(ratePerSecond int) {
	if ratePerSecond == 0 || q.dataChannel == nil {
		return
	}
	q.Lock()
	defer q.Unlock()
	q.doPause(true)
	q.dataChannel.SetRateLimit(ratePerSecond)
	q.rateLimitEnable = true
	q.doPause(false)
}

/**
* 关闭限速
 */
func (q *QueueProducerObject) DisableRateLimit() {
	if q.dataChannel != nil && q.rateLimitEnable {
		q.Lock()
		defer q.Unlock()
		q.doPause(true)
		q.dataChannel.CloseRateLimit()
		q.rateLimitEnable = false
		q.doPause(false)
	}
}

/**
* 发送消息
 */
func (q *QueueProducerObject) SendMessage(data []byte, async bool) error {
	defer func() {
		if err := recover(); err != nil {
			if _, ok := err.(error); ok {
				q.logFunc(util.ErrorLvl, "send message panic error:"+err.(error).Error())
			}
		}
	}()
	var addBackendStore bool = false
	var err error
	if async {
		addBackendStore = true
	} else {
		q.RLock()
		addBackendStore = false
		if q.queue != nil && q.queue.IsActive() {
			if q.rateLimitEnable {
				//开启流控,则直接开启异步发送模式
				addBackendStore = true
			} else {
				err = q.queue.SendMessage(data)
				if err != nil {
					//触发队列检测
					q.logFunc(util.ErrorLvl, "send message error:"+err.Error())
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
	q.pauseChan = make(chan bool)
	var pause bool = false
	var r chan []byte
	if q.dataChannel.Start() {
		//channel启动成功,则直接开始读取传输数据
		r = q.diskQueue.GetMessageChan()
	}
	for {
		select {
		case dataByte := <-r:
			if !q.dataChannel.Send(channel.Data{Value: string(dataByte)}) {
				q.logFunc(util.InfoLvl, "channel queue is full capacity")
				//发送失败,说明channel队列容量已满,尝试延迟重试
				r = nil
			}
		case <-checkQueueTicker.C:
			if pause {
				continue
			}
			if q.queue != nil {
				if q.queue.CheckActive() {
					q.logFunc(util.DebugLvl, "checked connected successed")
					if q.dataChannel.Start() {
						//channel启动成功,则直接开始读取传输数据
						if r == nil {
							r = q.diskQueue.GetMessageChan()
						}
					}
				} else {
					q.logFunc(util.InfoLvl, "checked connected failed")
					q.dataChannel.Pause()
					r = nil
				}
			}
		case <-q.checkQueueChan:
			if pause {
				continue
			}
			if q.queue != nil && q.queue.IsActive() {
				//主动发起的队列检测，仅当queue is active时才触发
				if !q.queue.CheckActive() {
					q.logFunc(util.InfoLvl, "checked connected failed")
					q.dataChannel.Pause()
					r = nil
				}
			}
		case pause = <-q.pauseChan:
			if pause {
				//禁止从 diskQueue 读数据,queueChannel消费正常
				r = nil
			}
		case <-q.exitChan:
			q.dataChannel.Stop()
			goto exit
		}
	}
exit:
	checkQueueTicker.Stop()
}

/**
* 初始化磁盘队列
 */
func (q *QueueProducerObject) initDiskQueue(topicName string, config config.DiskConfig) {
	q.diskQueue = queue.NewDiskQueue(config)
	q.diskQueue.SetTopic(topicName)
	if q.logFunc != nil {
		q.diskQueue.SetLogger(q.logFunc)
	}
}

/**
* 初始化数据传输管道(channel)
 */
func (q *QueueProducerObject) initDataChannel() {
	workerNum := q.config.ChannelConfig.WorkerNum
	if workerNum == 0 {
		workerNum = channel.DEFAULT_CHANNEL_WORKER_NUM
	}
	cfg := q.config.ChannelConfig
	cfg.Transaction.FtLogPath = q.diskQueue.GetDataPath()
	channel := channel.NewDataChannel(cfg, func(item channel.Data) {
		q.diskQueue.SendMessage([]byte(item.Value))
	}, nil, q.logFunc)
	for i := 0; i < workerNum; i++ {
		sender := backend.NewBatchProducer(func() (backend.BatchQueueProducer, error) {
			if q.queue == nil {
				return nil, fmt.Errorf("backend queue producer has not been init")
			}
			return q.queue.StartBatchProducer()
		})
		channel.AddSender(sender)
	}
	q.dataChannel = channel
}

func createQueueProducer(cfg config.QueueConfig) backend.QueueProducer {
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
