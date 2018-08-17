package backend

import (
	"sync/atomic"
	"time"

	"github.com/jmuyuyang/queue_proxy/util"
)

type QueueProduceChannel struct {
	queue        *util.BoundedQueue
	producerList []*TransactionProducer
	stopped      int32
}

/**
* 队列发送管道
 */
func NewQueueProduceChannel(queueSize int, workerNum int, onDroppedItem func(item interface{}), onProducerConstruct func() (BatchQueueProducer, error), logf util.LoggerFuncHandler) *QueueProduceChannel {
	if queueSize == 0 {
		queueSize = DEFAULT_CHANNEL_SIZE
	}
	q := &QueueProduceChannel{
		queue:        util.NewBoundedQueue(queueSize, onDroppedItem),
		producerList: make([]*TransactionProducer, 0),
		stopped:      int32(1),
	}
	if workerNum == 0 {
		workerNum = DEFAULT_CHANNEL_WORKER_NUM
	}
	for i := 0; i < workerNum; i++ {
		producer := NewTransactionProducer(func(item interface{}) {
			//非阻塞式尝试重新写回队列
			q.queue.Produce(item, 0*time.Second)
		}, onProducerConstruct, logf)
		q.producerList = append(q.producerList, producer)
	}
	return q
}

/**
* 启动channel producer,并进入消费模式
 */
func (q *QueueProduceChannel) Start() {
	if atomic.LoadInt32(&q.stopped) == 1 {
		var err error
		for _, producer := range q.producerList {
			err = producer.Start()
			if err != nil {
				return
			}
		}
		//全部启动成功才进入消费模式
		atomic.StoreInt32(&q.stopped, 0)
		for _, producer := range q.producerList {
			q.queue.AddConsumerWorker(producer)
		}
	}
}

/**
* 暂停queue channel消费
 */
func (q *QueueProduceChannel) Pause() {
	if atomic.LoadInt32(&q.stopped) == 0 {
		q.queue.StopConsumeWorker(len(q.producerList))
		atomic.StoreInt32(&q.stopped, 1)
	}
}

/**
* 停止queue channel
 */
func (q *QueueProduceChannel) Stop() {
	q.queue.Stop()
	atomic.StoreInt32(&q.stopped, 1)
}

/**
* 发送消息
 */
func (q *QueueProduceChannel) SendMessage(item []byte) bool {
	return q.queue.Produce(item, time.Duration(DEFAULT_PRODUCER_BATCH_SIZE)*time.Second)
}

/**
* 返回当前channel 队列长度
 */
func (q *QueueProduceChannel) Size() int {
	return q.queue.Size()
}

type TransactionProducer struct {
	itemBuffer          [][]byte
	lastCommit          time.Time
	onDroppedItem       func(item interface{})
	onProducerConstruct func() (BatchQueueProducer, error)
	producer            BatchQueueProducer
	logf                util.LoggerFuncHandler
}

/**
* 事务发送producer, 失败回滚
 */
func NewTransactionProducer(onDroppedItem func(item interface{}), onProducerConstruct func() (BatchQueueProducer, error), logf util.LoggerFuncHandler) *TransactionProducer {
	return &TransactionProducer{
		itemBuffer:          make([][]byte, 0),
		lastCommit:          time.Now(),
		onDroppedItem:       onDroppedItem,
		onProducerConstruct: onProducerConstruct,
		logf:                logf,
	}
}

/**
* 启动connect producer
 */
func (w *TransactionProducer) Start() error {
	if w.producer != nil {
		return nil
	}
	var err error
	w.producer, err = w.onProducerConstruct()
	if err != nil {
		w.logf(util.ErrorLvl, "start batch producer error: "+err.Error())
	}
	return err
}

func (w *TransactionProducer) Consume(item interface{}) {
	w.itemBuffer = append(w.itemBuffer, item.([]byte))
	if w.batchFullOrTimeout() {
		err := w.Commit()
		if err != nil {
			w.Rollback()
			//批量提交失败则进行一次producer重建
			w.Stop()
		}
		w.lastCommit = time.Now()
	}
}

/**
* 获取空闲检测超时时间
 */
func (w *TransactionProducer) IdleTimeout() time.Duration {
	return DEFAULT_PRODUCER_BATCH_INTERVAL * time.Second
}

/**
* 提交batch满或者超时
 */
func (w *TransactionProducer) batchFullOrTimeout() bool {
	if len(w.itemBuffer) >= DEFAULT_PRODUCER_BATCH_SIZE {
		return true
	}
	if time.Now().Sub(w.lastCommit).Seconds() > DEFAULT_PRODUCER_BATCH_INTERVAL {
		return true
	}
	return false
}

/**
* 空闲检测
 */
func (w *TransactionProducer) IdleCheck() {
	if time.Now().Sub(w.lastCommit).Seconds() > DEFAULT_QUEUE_IDLE_TIMEOUT {
		//上次提交提交超过链接空闲时间
		w.Stop()
	}
	if len(w.itemBuffer) > 0 && w.batchFullOrTimeout() {
		err := w.Commit()
		if err != nil {
			w.Rollback()
			//批量提交失败则进行一次producer重建
			w.Stop()
		}
		w.lastCommit = time.Now()
	}
}

/**
* 停止producer
 */
func (w *TransactionProducer) Stop() {
	util.WithRecover(func() {
		if len(w.itemBuffer) > 0 {
			err := w.Commit()
			if err != nil {
				w.Rollback()
			}
		}
		if w.producer != nil {
			w.producer.Stop()
			w.producer = nil
		}
	}, func(err error) {
		w.logf(util.ErrorLvl, "stop batch producer error: "+err.Error())
		w.producer = nil
	})
}

/**
* 批量提交message batch
 */
func (w *TransactionProducer) Commit() error {
	if w.producer == nil {
		err := w.Start()
		if err != nil {
			return err
		}
	}
	if len(w.itemBuffer) > 0 {
		err := w.producer.SendMessages(w.itemBuffer)
		if err == nil {
			w.itemBuffer = make([][]byte, 0)
		} else {
			w.logf(util.ErrorLvl, "send batch messages error: "+err.Error())
		}
		return err
	}
	return nil
}

/**
* 批量回滚message batch
 */
func (w *TransactionProducer) Rollback() {
	if w.onDroppedItem != nil {
		for _, item := range w.itemBuffer {
			w.onDroppedItem(item)
		}
	}
	w.itemBuffer = make([][]byte, 0)
}
