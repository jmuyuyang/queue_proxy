package channel

import (
	"sync/atomic"
	"time"

	"github.com/jmuyuyang/queue_proxy/config"
	"github.com/jmuyuyang/queue_proxy/queue"
	"github.com/jmuyuyang/queue_proxy/util"
)

const (
	DEFAULT_CHANNEL_SIZE       = 100
	DEFAULT_CHANNEL_WORKER_NUM = 1

	DEFAULT_CHANNEL_TRANSACTION_LEN            = 100    //最大一个batch100条
	DEFAULT_CHANNEL_TRANSACTION_SIZE           = 131072 //最大一个batch128KB
	DEFAULT_CHANNEL_TRANSACTION_TIMEOUT        = 10
	DEFAULT_CHANNEL_TRANSACTION_COMMIT_TIMEOUT = 10
)

type Data struct {
	Value string                 `json:"val"`
	Attr  map[string]interface{} `json:"-"`
}

type Channel struct {
	queue        *queue.BoundedQueue
	transManager *TransactionManager
	senderList   []ChannelSender
	logf         util.LoggerFuncHandler
	exitChan     chan struct{}
	stopped      int32
}

type ChannelSender interface {
	Start() error
	Send([]Data) error
	IdleCheck()
	Stop()
}

/**
* 创建新的数据 channel
* ftLogPath 错误回滚队列路径
* onDroppedItem 队列满时数据丢弃方式
 */
func NewDataChannel(cfg config.ChannelConfig, onDroppedItem func(item Data), onMetaSync func(item Data), logf util.LoggerFuncHandler) *Channel {
	queueSize := cfg.Size
	if queueSize == 0 {
		queueSize = DEFAULT_CHANNEL_SIZE
	}
	return &Channel{
		queue: queue.NewBoundedQueue(queueSize, func(item interface{}) {
			onDroppedItem(item.(Data))
		}),
		transManager: NewTransactionManager(cfg.Transaction, onMetaSync, logf),
		senderList:   make([]ChannelSender, 0),
		logf:         logf,
		exitChan:     make(chan struct{}),
		stopped:      int32(1),
	}
}

/**
* 启动channel
 */
func (q *Channel) Start() {
	if atomic.LoadInt32(&q.stopped) == 1 {
		for _, sender := range q.senderList {
			err := sender.Start()
			if err != nil {
				q.logf(util.ErrorLvl, "start sender worker error:"+err.Error())
				return
			}
		}
		//sender全部自启动成功才进入消费模式
		atomic.StoreInt32(&q.stopped, 0)
		q.transManager.Start(len(q.senderList))
		for _, sender := range q.senderList {
			q.startSenderWorker(sender)
		}
		q.queue.AddConsumeWorker(q.transManager)
	}
}

/**
* 添加消费worker
 */
func (q *Channel) AddSender(sender ChannelSender) {
	q.senderList = append(q.senderList, sender)
}

/**
* 启动sender worker
 */
func (q *Channel) startSenderWorker(sender ChannelSender) {
	go func() {
		idleTicker := time.NewTicker(5 * time.Second)
		for {
			if atomic.LoadInt32(&q.stopped) != 0 {
				sender.Stop()
				idleTicker.Stop()
				q.exitChan <- struct{}{}
				return
			}
			select {
			case tran := <-q.transManager.TranChan:
				err := sender.Send(tran.Buffer)
				if err != nil {
					q.logf(util.ErrorLvl, "send batch messages error:"+err.Error())
					q.transManager.Rollback(tran)
				} else {
					q.transManager.Confirm(tran)
					q.logf(util.DebugLvl, "send batch messages successful")
				}
			case <-idleTicker.C:
				sender.IdleCheck()
			}
		}
	}()
}

/**
* 发送消息
 */
func (q *Channel) Send(data Data) bool {
	return q.queue.Produce(data, time.Duration(DEFAULT_CHANNEL_TRANSACTION_TIMEOUT)*time.Second)
}

/**
* 获取当前队列长度
 */
func (q *Channel) Size() int {
	return q.queue.Size()
}

/**
* 获取当前队列容量
 */
func (q *Channel) Capacity() int {
	return q.queue.Capacity()
}

/**
* 暂停channel sender消费
 */
func (q *Channel) Pause() {
	if atomic.LoadInt32(&q.stopped) == 0 {
		//先停止队列数据消费,再停止事务提交
		q.queue.StopConsumeWorker()
		atomic.StoreInt32(&q.stopped, 1)
		for i := 0; i < len(q.senderList); i++ {
			<-q.exitChan
		}
		q.transManager.Pause()
	}
}

/**
* 停止channel
 */
func (q *Channel) Stop() {
	if atomic.LoadInt32(&q.stopped) == 0 {
		//先停止队列数据写入/消费,再停止事务提交
		q.queue.Stop()
		atomic.StoreInt32(&q.stopped, 1)
		for i := 0; i < len(q.senderList); i++ {
			<-q.exitChan
		}
		q.transManager.Stop()
	}
}
