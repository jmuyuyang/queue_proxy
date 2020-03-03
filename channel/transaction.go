package channel

import (
	"sync"
	"time"

	"github.com/jmuyuyang/queue_proxy/config"
	"github.com/jmuyuyang/queue_proxy/queue"
	"github.com/jmuyuyang/queue_proxy/util"
	"github.com/json-iterator/go"
	"github.com/satori/go.uuid"
)

var jsontool = jsoniter.ConfigCompatibleWithStandardLibrary

type TransactionManager struct {
	cfg              config.TransactionConfig
	commitLock       sync.Mutex
	uncommitedTrans  map[string]*TransactionBatch
	confirmLock      sync.Mutex
	unconfirmedTrans map[string]TransactionBatch
	backupQueue      *queue.DiskQueue
	onMetaSync       func(Data)
	TranChan         chan TransactionBatch
	stopCh           chan struct{}
	waitGroup        util.WaitGroupWrapper
	logf             util.LoggerFuncHandler
}

type TransactionBatch struct {
	Id        string    `json:"id"`
	BatchSize int64     `json:"-"`
	Retry     int       `json:"retry"`
	Buffer    []Data    `json:"datas"`
	StartTime time.Time `json:"-"`
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
	diskCfg := config.DiskConfig{
		Path:         cfg.FtLogPath,
		FlushTimeout: 2,
		CompressType: "gzip",
		MaxMsgSize:   cfg.MaxMsgSize,
	}
	var bq *queue.DiskQueue = queue.NewDiskQueue(diskCfg, logf)
	bq.SetTopic("channel")
	bq.Start()
	return &TransactionManager{
		cfg:              cfg,
		uncommitedTrans:  make(map[string]*TransactionBatch, 0),
		unconfirmedTrans: make(map[string]TransactionBatch, 0),
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
					tran.Retry += 1
					select {
					case t.TranChan <- tran:
						t.confirmLock.Lock()
						t.unconfirmedTrans[tran.Id] = tran
						t.confirmLock.Unlock()
						t.logf(util.InfoLvl, "commit transaction from backup queue: "+tran.Id)
					case <-time.After(time.Duration(t.cfg.CommitTimeout) * time.Second):
						//事务提交超时,重试次数回滚
						tran.Retry -= 1
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
	t.wholeRollback()
}

/**
* 停止事务管理器
 */
func (t *TransactionManager) Stop() {
	close(t.stopCh)
	t.waitGroup.Wait()
	close(t.TranChan)
	t.wholeRollback()
	t.backupQueue.Stop()
}

/**
* 所有未确认/提交事务整体回滚
 */
func (t *TransactionManager) wholeRollback() {
	t.commitLock.Lock()
	if len(t.uncommitedTrans) > 0 && t.onMetaSync == nil {
		//如果没有元数据同步机制, 则当前未提交事务需要回滚
		for gid, tran := range t.uncommitedTrans {
			t.rollback(*tran)
			delete(t.uncommitedTrans, gid)
		}
	}
	t.commitLock.Unlock()
	t.confirmLock.Lock()
	if len(t.unconfirmedTrans) > 0 {
		//未确认事务回滚
		for tid, tran := range t.unconfirmedTrans {
			t.rollback(tran)
			delete(t.unconfirmedTrans, tid)
		}
	}
	t.confirmLock.Unlock()
}

/**
* 启动新事务
 */
func (t *TransactionManager) StartTransaction() *TransactionBatch {
	tran := &TransactionBatch{
		Id:        uuid.NewV4().String(),
		Retry:     0,
		Buffer:    make([]Data, 0),
		StartTime: time.Now(),
	}
	t.logf(util.InfoLvl, "start new transaction: "+tran.Id)
	return tran
}

func (t *TransactionManager) Consume(item interface{}) {
	data, ok := item.(Data)
	if ok {
		var gid string = "default"
		if data.Gid != "" {
			gid = data.Gid
		}
		//同一个gid在同一个事务批次,满足多分片reader->单channel
		t.commitLock.Lock()
		curTran, ok := t.uncommitedTrans[gid]
		if !ok {
			curTran = t.StartTransaction()
		}
		curTran.Append(data)
		t.uncommitedTrans[gid] = curTran
		if t.needCommitTran(*curTran) {
			t.Commit(*curTran)
			delete(t.uncommitedTrans, gid)
		}
		t.commitLock.Unlock()
	}
}

/**
* 判断事务批次是否需要提交
 */
func (t *TransactionManager) needCommitTran(tran TransactionBatch) bool {
	if len(tran.Buffer) == 0 {
		return false
	}

	if len(tran.Buffer) >= t.cfg.BatchLen {
		//批次长度超限制
		return true
	}
	if tran.BatchSize >= t.cfg.BatchDataSize {
		//批次大小超限制
		return true
	}
	if time.Now().Sub(tran.StartTime).Seconds() > float64(t.cfg.BatchInterval) {
		//最长事务留存时间超过限制
		return true
	}
	return false
}

func (t *TransactionManager) IdleCheck() {
	t.commitLock.Lock()
	for gid, tran := range t.uncommitedTrans {
		if t.needCommitTran(*tran) {
			t.Commit(*tran)
			delete(t.uncommitedTrans, gid)
		}
	}
	t.commitLock.Unlock()
}

/**
* 提交当前事务
 */
func (t *TransactionManager) Commit(tran TransactionBatch) {
	select {
	case t.TranChan <- tran:
		if t.onMetaSync != nil {
			t.onMetaSync(tran.LastData())
		}
		t.logf(util.InfoLvl, "commit transaction: "+tran.Id)
		t.confirmLock.Lock()
		t.unconfirmedTrans[tran.Id] = tran
		t.confirmLock.Unlock()
	case <-time.After(time.Duration(t.cfg.CommitTimeout) * time.Second):
		//事务提交超时
		t.logf(util.InfoLvl, "commit transaction: "+tran.Id+" timeout")
		t.rollback(tran)
	}
}

/**
* 确认某批次事务
 */
func (t *TransactionManager) Confirm(tran TransactionBatch) {
	t.confirmLock.Lock()
	defer t.confirmLock.Unlock()
	if _, ok := t.unconfirmedTrans[tran.Id]; ok {
		delete(t.unconfirmedTrans, tran.Id)
		t.logf(util.InfoLvl, "confirm transaction: "+tran.Id)
	}
}

/**
* 回滚某批次事务
 */
func (t *TransactionManager) Rollback(tran TransactionBatch) {
	t.confirmLock.Lock()
	defer t.confirmLock.Unlock()
	if _, ok := t.unconfirmedTrans[tran.Id]; ok {
		if t.cfg.FailSleep > 0 {
			//失败重试休眠机制,阻塞式,避免崩溃式失败
			time.Sleep(time.Duration(t.cfg.FailSleep) * time.Second)
		}
		t.rollback(tran)
		delete(t.unconfirmedTrans, tran.Id)
	}
}

func (t *TransactionManager) rollback(tran TransactionBatch) {
	if t.cfg.MaxRetry > 0 && tran.Retry >= t.cfg.MaxRetry {
		//超过最大重试次数
		t.logf(util.WarnLvl, "transaction: "+tran.Id+" rollback above max retry time")
		return
	}
	transBytes, err := tran.MarshalJson()
	if err == nil {
		t.backupQueue.SendMessage(transBytes)
	}
	t.logf(util.InfoLvl, "rollback transaction: "+tran.Id)
}
