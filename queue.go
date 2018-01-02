package queue

import (
	"context"
	"time"

	"github.com/jmuyuyang/queue_proxy/rateio"

	seelog "github.com/cihub/seelog"
)

const IDLE_TIMEOUT = 60 * 10
const CHECK_QUEUE_TIMEOUT = 5 * time.Second
const CHECK_QUEUE_CHAIN_BUFFER = 3

type QueueProducer interface {
	StartPipeline() (PipelineQueueProducer, error)
	SetTopic(string)
	SendMessage([]byte) error
	CheckQueue() bool
	IsActive() bool
}

type PipelineQueueProducer interface {
	SendMessage([]byte) error
	Flush() error
	Close() error
}

type QueueConfig struct {
	Type  string      `yaml:"type"`
	Redis RedisConfig `yaml:"redis"`
	Kafka KafkaConfig `yaml:"kafka"`
	Disk  DiskConfig  `yaml:"disk"`
}

type QueueProducerObject struct {
	topic          string
	config         QueueConfig
	queue          QueueProducer
	backend        *DiskQueue
	rateController *rateio.Controller
	checkQueueChan chan int
}

func NewQueueProducer(topicName string, config QueueConfig, logger seelog.LoggerInterface) *QueueProducerObject {
	senderObj := &QueueProducerObject{
		topic:          topicName,
		config:         config,
		queue:          CreateQueueProducer(config),
		checkQueueChan: make(chan int, CHECK_QUEUE_CHAIN_BUFFER),
	}
	if config.Disk.Path != "" {
		diskQueue, err := senderObj.createDiskQueue()
		if err != nil {
			logger.Error(err)
		} else {
			senderObj.backend = diskQueue
			if logger != nil {
				senderObj.backend.SetLogger(logger)
			}
		}
	}
	senderObj.SetTopic(topicName)
	return senderObj
}

func (t *QueueProducerObject) createDiskQueue() (*DiskQueue, error) {
	diskQueueName := t.config.Disk.Prefix + "-" + t.topic
	return NewDiskQueue(diskQueueName, t.config.Disk.Path, time.Duration(t.config.Disk.FlushTimeout))
}

/**
* 设置队列类型
 */
func (t *QueueProducerObject) SetQueueType(queueType string) {
	t.config.Type = queueType
	t.queue = CreateQueueProducer(t.config)
}

/**
* 设置publish 主题
 */
func (t *QueueProducerObject) SetTopic(topicName string) {
	t.topic = topicName
	t.queue.SetTopic(topicName)
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
func (sd *QueueProducerObject) SendMessage(data []byte) bool {
	addBackendStore := false
	if sd.queue != nil && sd.queue.IsActive() {
		if sd.rateController != nil && !sd.rateController.Assign(false) {
			//超过限速
			addBackendStore = true
		} else {
			err := sd.queue.SendMessage(data)
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
		if sd.backend != nil {
			sd.backend.WriteChan <- data
		}
		return false
	} else {
		return true
	}
}

func (sd *QueueProducerObject) StartBackend(ctx context.Context) {
	if sd.backend == nil {
		return
	}
	checkQueueTicker := time.NewTicker(CHECK_QUEUE_TIMEOUT) //监测队列链接是否正常
	r := sd.backend.ReadChan
	var pipelineQueue PipelineQueueProducer
	var err error
	for {
		select {
		case dataByte := <-r:
			if pipelineQueue == nil {
				pipelineQueue, err = sd.queue.StartPipeline()
			}
			if err != nil {
				//创建pipeline队列失败,则直接禁止读取disk queue
				sd.backend.WriteChan <- dataByte
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
			if sd.queue != nil && sd.queue.CheckQueue() {
				r = sd.backend.ReadChan
			} else {
				r = nil
			}
		case <-sd.checkQueueChan:
			if sd.queue == nil || sd.queue.IsActive() && !sd.queue.CheckQueue() {
				if pipelineQueue != nil {
					pipelineQueue.Close()
					pipelineQueue = nil
				}
				r = nil
			}
		case <-ctx.Done():
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
	sd.backend.Stop()
}

func CreateQueueProducer(config QueueConfig) QueueProducer {
	if config.Type == "redis" {
		return NewRedisQueue(config.Redis)
	}
	if config.Type == "kafka" {
		return NewKafkaQueue(config.Kafka)
	}
	return nil
}
