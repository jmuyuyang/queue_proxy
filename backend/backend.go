package backend

import (
	"strconv"
	"time"

	"sync"
	"fmt"
	"github.com/jmuyuyang/queue_proxy/channel"
	"github.com/jmuyuyang/queue_proxy/util"
	metrics "github.com/rcrowley/go-metrics"
)

var doOnce sync.Once

const (
	ClientIDLength = 16

	DEFAULT_QUEUE_IDLE_TIMEOUT    = 60 * 10
	DEFAULT_QUEUE_ABANDON_TIMEOUT = 60 * 10
)

func init() {
	doOnce.Do(func() {
		//禁用kafka等队列库自带的metric统计
		metrics.UseNilMetrics = true
	})
}

/*
type MessageID string

type Message struct {
	ClientID [ClientIDLength]byte `json:"client_id"`
	ID       MessageID            `json:"id"`
	Body     []byte               `json:"body"`
	pri      int64
	index    int
}
*/

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

type BatchProducer struct {
	lastSend            time.Time
	onProducerConstruct func() (BatchQueueProducer, error)
	producer            BatchQueueProducer
	logFunc             util.LoggerFuncHandler
}

/**
* 事务发送producer, 失败回滚
 */
func NewBatchProducer(logFunc util.LoggerFuncHandler, onProducerConstruct func() (BatchQueueProducer, error)) *BatchProducer {
	return &BatchProducer{
		lastSend:            time.Now(),
		onProducerConstruct: onProducerConstruct,
		logFunc:             logFunc,
	}
}

/**
* 启动connect producer
 */
func (w *BatchProducer) Start() error {
	if w.producer != nil {
		return nil
	}
	var err error
	w.producer, err = w.onProducerConstruct()
	return err
}

func (w *BatchProducer) Send(items []channel.Data) error {
	if w.producer == nil {
		err := w.Start()
		if err != nil {
			return err
		}
	}
	msgList := make([][]byte, 0)
	for _, item := range items {
		msgList = append(msgList, []byte(item.Value))
	}
	var err error
	util.WithRecover(func() {
		startTime := time.Now().UnixNano()
		err = w.producer.SendMessages(msgList)
		if err != nil {
			fmt.Println(msgList)
			//批量提交失败则进行一次producer重建
			w.producer.Stop()
			w.producer = nil
		}
		endTime := time.Now().UnixNano()
		w.logFunc(util.DebugLvl, "send data take time Millisecond "+strconv.Itoa(int(endTime-startTime)/1e6))
	}, func(err error) {
		w.producer = nil
	})
	w.lastSend = time.Now()
	return err
}

/**
* 空闲检测
 */
func (w *BatchProducer) IdleCheck() {
	if time.Now().Sub(w.lastSend).Seconds() > DEFAULT_QUEUE_IDLE_TIMEOUT {
		//上次提交提交超过链接空闲时间
		w.Stop()
	}
}

/**
* 停止producer
 */
func (w *BatchProducer) Stop() {
	util.WithRecover(func() {
		if w.producer != nil {
			w.producer.Stop()
			w.producer = nil
		}
	}, func(err error) {
		w.producer = nil
	})
}
