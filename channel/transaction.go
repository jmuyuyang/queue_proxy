package channel

import (
	"sync"
	"time"

	"github.com/json-iterator/go"

	"github.com/jmuyuyang/queue_proxy/config"
	"github.com/jmuyuyang/queue_proxy/queue"
	"github.com/jmuyuyang/queue_proxy/util"
	"github.com/satori/go.uuid"
)

var jsontool = jsoniter.ConfigCompatibleWithStandardLibrary

type TransactionManager struct {
	cfg              config.TransactionConfig
	curTrans         *TransactionBatch
	transLock        sync.Mutex
	unconfirmedTrans map[string]TransactionBatch
	backupQueue      *queue.DiskQueue
	lastCommit       time.Time
	onMetaSync       func(Data)
	TranChan         chan TransactionBatch
	stopCh           chan struct{}
	waitGroup        util.WaitGroupWrapper
	logf             util.LoggerFuncHandler
}

type TransactionBatch struct {
	Id        string `json:"id"`
	Retry     int    `json:"retry"`
	BatchSize int64  `json:"-"`
	Buffer    []Data `json:"datas"`
}

/**
* 事务批次数据json化
 */
func (t *TransactionBatch) MarshalJson() ([]byte, error) {
	return jsontool.Marshal(t)
}

/**
* 添加数据进事务批次
 */
func (t *TransactionBatch) Append(data Data) {
	t.Buffer = append(t.Buffer, data)
	t.BatchSize += int64(len(data.Value))
}

/**
* 返回该事务批次最后一条数据
 */
func (t *TransactionBatch) LastData() Data {
	return t.Buffer[len(t.Buffer)-1]
}

func NewTransactionManager(cfg config.TransactionConfig, onMetaSync func(item Data), logf util.LoggerFuncHandler) *TransactionManager {
	if cfg.BatchLen == 0 {
		cfg.BatchLen = DEFAULT_CHANNEL_TRANSACTION_LEN
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = int64(DEFAULT_CHANNEL_TRANSACTION_SIZE)
	}
	if cfg.CommitTimeout == 0 {
		cfg.CommitTimeout = DEFAULT_CHANNEL_TRANSACTION_COMMIT_TIMEOUT
	}
	diskCfg := config.DiskConfig{
		Path:         cfg.FtLogPath,
		FlushTimeout: 2,
		CompressType: "gzip",
	}
	var bq *queue.DiskQueue = queue.NewDiskQueue(diskCfg)
	bq.SetTopic("channel")
	bq.SetLogger(logf)
	bq.Start()
	return &TransactionManager{
		cfg:              cfg,
		unconfirmedTrans: make(map[string]TransactionBatch, 0),
		lastCommit:       time.Now(),
		backupQueue:      bq,
		onMetaSync:       onMetaSync,
		stopCh:           make(chan struct{}),
		logf:             logf,
	}
}

/**
* 启动事务管理器
 */
func (t *TransactionManager) Start(tranBufferSize int) {
	t.TranChan = make(chan TransactionBatch, tranBufferSize)
	t.waitGroup.Wrap(func() {
		for {
			select {
			case tranBytes := <-t.backupQueue.GetMessageChan():
				//重试回滚事务
				tran := TransactionBatch{}
				err := jsontool.Unmarshal(tranBytes, &tran)
				if err == nil {
					select {
					case t.TranChan <- tran:
						t.transLock.Lock()
						t.unconfirmedTrans[tran.Id] = tran
						t.transLock.Unlock()
						t.logf(util.InfoLvl, "commit transaction from backup queue: "+tran.Id)
					case <-time.After(time.Duration(t.cfg.CommitTimeout) * time.Second):
						//事务提交超时
						transBytes, err := tran.MarshalJson()
						if err == nil {
							t.backupQueue.SendMessage(transBytes)
							t.logf(util.InfoLvl, "commit transaction: "+tran.Id+" timeout")
						}
					}
				}
			case <-t.stopCh:
				return
			}
		}
	})
}

/**
* 暂停事务管理器
 */
func (t *TransactionManager) Pause() {
	t.stopCh <- struct{}{}
	t.waitGroup.Wait()
	close(t.TranChan)
	t.transLock.Lock()
	if t.curTrans != nil && t.onMetaSync == nil {
		//如果没有元数据同步机制, 则当前未提交事务需要回滚
		t.rollback(*t.curTrans)
		t.curTrans = nil
	}
	if len(t.unconfirmedTrans) > 0 {
		//未确认事务回滚
		for _, tran := range t.unconfirmedTrans {
			t.rollback(tran)
		}
	}
	t.transLock.Unlock()
}

/**
* 停止事务管理器
 */
func (t *TransactionManager) Stop() {
	close(t.stopCh)
	t.waitGroup.Wait()
	close(t.TranChan)
	t.transLock.Lock()
	if t.curTrans != nil && t.onMetaSync == nil {
		//如果没有元数据同步机制, 则当前未提交事务需要回滚
		t.rollback(*t.curTrans)
		t.curTrans = nil
	}
	if len(t.unconfirmedTrans) > 0 {
		//未确认事务回滚
		for _, tran := range t.unconfirmedTrans {
			t.rollback(tran)
		}
	}
	t.transLock.Unlock()
	t.backupQueue.Stop()
}

/**
* 启动新事务
 */
func (t *TransactionManager) StartTransaction() {
	t.curTrans = &TransactionBatch{
		Id:     uuid.NewV4().String(),
		Buffer: make([]Data, 0),
	}
	t.logf(util.InfoLvl, "start new transaction: "+t.curTrans.Id)
}

func (t *TransactionManager) Consume(data interface{}) {
	if _, ok := data.(Data); ok {
		if t.curTrans == nil {
			//启动新的事务批次
			t.StartTransaction()
		}
		t.curTrans.Append(data.(Data))
		if t.needCommitTran() {
			t.Commit()
		}
	}
}

/**
* 判断事务批次是否需要提交
 */
func (t *TransactionManager) needCommitTran() bool {
	if t.curTrans == nil {
		return false
	}
	if len(t.curTrans.Buffer) >= t.cfg.BatchLen {
		//批次长度超限制
		return true
	}
	if t.curTrans.BatchSize >= t.cfg.BatchSize {
		//批次大小超限制
		return true
	}
	if len(t.curTrans.Buffer) > 0 && time.Now().Sub(t.lastCommit).Seconds() > DEFAULT_CHANNEL_TRANSACTION_TIMEOUT {
		//最长事务留存时间超过限制
		return true
	}
	return false
}

func (t *TransactionManager) IdleCheck() {
	if t.needCommitTran() {
		t.Commit()
	}
}

/**
* 提交当前事务
 */
func (t *TransactionManager) Commit() {
	if t.curTrans != nil {
		tran := *t.curTrans
		t.transLock.Lock()
		t.unconfirmedTrans[t.curTrans.Id] = tran
		t.transLock.Unlock()
		select {
		case t.TranChan <- tran:
			if t.onMetaSync != nil {
				t.onMetaSync(t.curTrans.LastData())
			}
			t.lastCommit = time.Now()
			t.logf(util.InfoLvl, "commit transaction: "+t.curTrans.Id)
		case <-time.After(time.Duration(t.cfg.CommitTimeout) * time.Second):
			//事务提交超时
			t.logf(util.InfoLvl, "commit transaction: "+t.curTrans.Id+" timeout")
			t.transLock.Lock()
			t.rollback(tran)
			t.transLock.Unlock()
		}
		t.curTrans = nil
	}
}

/**
* 确认某批次事务
 */
func (t *TransactionManager) Confirm(tran TransactionBatch) {
	t.transLock.Lock()
	defer t.transLock.Unlock()
	if _, ok := t.unconfirmedTrans[tran.Id]; ok {
		delete(t.unconfirmedTrans, tran.Id)
		t.logf(util.InfoLvl, "confirm transaction: "+tran.Id)
	}
}

/**
* 回滚某批次事务
 */
func (t *TransactionManager) Rollback(tran TransactionBatch) {
	t.transLock.Lock()
	defer t.transLock.Unlock()
	if _, ok := t.unconfirmedTrans[tran.Id]; ok {
		if t.cfg.FailSleep > 0 {
			//失败重试休眠机制,阻塞式,避免崩溃式失败
			time.Sleep(time.Duration(t.cfg.FailSleep) * time.Second)
		}
		t.rollback(tran)
	}
}

func (t *TransactionManager) rollback(tran TransactionBatch) {
	transBytes, err := tran.MarshalJson()
	if err == nil {
		t.backupQueue.SendMessage(transBytes)
	}
	delete(t.unconfirmedTrans, tran.Id)
	t.logf(util.InfoLvl, "rollback transaction: "+tran.Id)
}
