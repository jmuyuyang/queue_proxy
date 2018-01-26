package backend

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	rd "github.com/garyburd/redigo/redis"
	"github.com/jmuyuyang/queue_proxy/util"
)

const (
	DEFAULT_BUFFER_SIZE     = 10
	REDIS_POOL_IDLE_TIMEOUT = 60 * 10
	REDIS_CONN_READ_TIMEOUT = 30 //读超时10s,用于brpop操作
	REDIS_POOL_PING_TIMEOUT = 5  //PING超时
)

type redisQueue struct {
	pool   *rd.Pool
	config RedisConfig
	topic  string
	enable bool
}

type RedisQueueProducer struct {
	redisQueue
}

type RedisPipelineProducer struct {
	conn          rd.Conn
	topic         string
	bufferSize    int32
	curBufferSize int32
}

type RedisQueueConsumer struct {
	redisQueue
	idChan           chan [MsgIDLength]byte
	exitChan         chan int
	MessageChan      chan *Message
	inFlightPQ       inFlightPqueue
	inFlightMutex    sync.Mutex
	inFlightMessages map[MessageID]*FlightMessage
	waitGroup        util.WaitGroupWrapper
}

type RedisConfig struct {
	Bind        string `yaml:"bind"`
	Timeout     int    `yaml:"timeout"`
	Size        int    `yaml:"size"`
	Topic       string `yaml:"topic"`
	MaxQueueLen int    `yaml:"max_queue_len"`
}

func createRedisQueuePool(host string, connTimeout, idleTimeout time.Duration, maxConn int) *rd.Pool {
	pool := &rd.Pool{
		MaxIdle:     maxConn,
		IdleTimeout: idleTimeout,
		Dial: func() (rd.Conn, error) {
			c, err := rd.Dial("tcp", host,
				rd.DialConnectTimeout(connTimeout),
				rd.DialReadTimeout(connTimeout),
				rd.DialWriteTimeout(connTimeout))
			if err != nil {
				return nil, err
			}
			return c, err
		},
	}
	return pool
}

func newRedisQueue(config RedisConfig) redisQueue {
	timeout := time.Duration(config.Timeout) * time.Second
	return redisQueue{
		pool:   createRedisQueuePool(config.Bind, timeout, REDIS_POOL_IDLE_TIMEOUT*time.Second, config.Size),
		config: config,
		topic:  config.Topic,
		enable: true,
	}
}

func (q *redisQueue) SetTopic(topic string) {
	q.topic = topic
}

func (q *redisQueue) IsActive() bool {
	return q.enable
}

/**
* 检查队列是否正常启用
 */
func (q *redisQueue) CheckQueue() bool {
	if !q.checkConn() {
		//链接非活动
		q.enable = false
		return false
	}
	if !q.checkQueueLen() {
		//队列长度超过限制
		q.enable = false
		return false
	}
	q.enable = true
	return true
}

/**
* 检查队列链接是否正常
 */
func (q *redisQueue) checkConn() bool {
	conn := q.pool.Get()
	defer conn.Close()
	_, err := rd.DoWithTimeout(conn, time.Duration(REDIS_POOL_PING_TIMEOUT)*time.Second, "PING")
	if err != nil {
		return false
	} else {
		return true
	}
}

/**
* 检查队列长度
 */
func (q *redisQueue) checkQueueLen() bool {
	conn := q.pool.Get()
	defer conn.Close()
	queueLen, err := rd.Int(conn.Do("LLEN", q.topic))
	if err != nil {
		return false
	} else {
		if q.config.MaxQueueLen > 0 && queueLen > q.config.MaxQueueLen {
			return false
		} else {
			return true
		}
	}
}

func NewRedisQueueProducer(config RedisConfig) *RedisQueueProducer {
	return &RedisQueueProducer{
		redisQueue: newRedisQueue(config),
	}
}

func (q *RedisQueueProducer) SendMessage(data []byte) error {
	conn := q.pool.Get()
	defer conn.Close()
	_, err := conn.Do("LPUSH", q.topic, string(data))
	return err
}

/**
* 开启pipeline
 */
func (q *RedisQueueProducer) StartPipeline() (PipelineQueueProducer, error) {
	conn := q.pool.Get()
	err := conn.Send("MULTI")
	if err != nil {
		return nil, err
	}
	pipelineQueue := &RedisPipelineProducer{
		conn:          conn,
		topic:         q.config.Topic,
		bufferSize:    int32(DEFAULT_BUFFER_SIZE),
		curBufferSize: int32(0),
	}
	return pipelineQueue, nil
}

func (q *RedisPipelineProducer) SendMessage(log []byte) error {
	q.conn.Send("LPUSH", q.topic, string(log))
	atomic.AddInt32(&q.curBufferSize, 1)
	if atomic.LoadInt32(&q.curBufferSize) >= q.bufferSize {
		err := q.conn.Send("EXEC")
		atomic.StoreInt32(&q.curBufferSize, int32(0))
		return err
	}
	return nil
}

/**
* 刷新piplien add log
 */
func (q *RedisPipelineProducer) Flush() error {
	if atomic.LoadInt32(&q.curBufferSize) >= int32(0) {
		err := q.conn.Send("EXEC")
		atomic.StoreInt32(&q.curBufferSize, 0)
		return err
	}
	return nil
}

func (q *RedisPipelineProducer) Close() error {
	defer q.conn.Close()
	return q.Flush()
}

func NewRedisQueueConsumer(config RedisConfig) *RedisQueueConsumer {
	return &RedisQueueConsumer{
		redisQueue:       newRedisQueue(config),
		idChan:           make(chan [MsgIDLength]byte),
		exitChan:         make(chan int),
		MessageChan:      make(chan *Message),
		inFlightPQ:       newInFlightPqueue(10),
		inFlightMessages: make(map[MessageID]*FlightMessage),
	}
}

func (c *RedisQueueConsumer) Start() {
	c.waitGroup.Wrap(func() {
		go c.queueScanWorker()
	})
	c.waitGroup.Wrap(func() {
		go c.idPump()
	})
	c.startConsumerWorker()
}

func (c *RedisQueueConsumer) startConsumerWorker() {
	c.waitGroup.Wrap(func() {
		go c.queueConsumerWorker()
	})
}

func (c *RedisQueueConsumer) AckMessage(msgId MessageID) error {
	conn := c.pool.Get()
	defer conn.Close()
	key := fmt.Sprintf("%s_%s", c.topic, msgId)
	_, err := conn.Do("DEL", key)
	if err == nil {
		c.removeFromInFlightPQ(msgId)
		return nil
	}
	return err
}

func (c *RedisQueueConsumer) GetMessageChan() chan *Message {
	return c.MessageChan
}

func (c *RedisQueueConsumer) Stop() {
	close(c.exitChan)
	c.waitGroup.Wait()
}

/**
* guid 生成器(message id)
 */
func (c *RedisQueueConsumer) idPump() {
	factory := &util.GuidFactory{}
	for {
		id, err := factory.NewGUID(0)
		if err != nil {
			continue
		}
		select {
		case c.idChan <- id.Hex():
		case <-c.exitChan:
			goto exit
		}
	}
exit:
}

func (c *RedisQueueConsumer) getMessage(wait bool) (*Message, error) {
	conn := c.pool.Get()
	defer conn.Close()
	var data []byte
	var err error
	if wait {
		s, err := rd.ByteSlices(rd.DoWithTimeout(conn, time.Duration(REDIS_CONN_READ_TIMEOUT)*time.Second, "BRPOP", c.topic, REDIS_CONN_READ_TIMEOUT))
		if err == nil {
			data = s[1]
		}
	} else {
		data, err = rd.Bytes(conn.Do("RPOP", c.topic))
	}
	if len(data) > 0 {
		msgId := <-c.idChan
		message := &Message{
			ID:   MessageID(msgId),
			Body: data,
		}
		inFlightKey := fmt.Sprintf("%s_%s", c.topic, message.ID)
		conn.Do("SET", inFlightKey, data)
		return message, err
	}
	return nil, err
}

func (c *RedisQueueConsumer) addToInFlightPQ(msg *Message, timeout time.Duration) {
	c.inFlightMutex.Lock()
	now := time.Now()
	flightMsg := &FlightMessage{
		ID:    msg.ID,
		pri:   now.Add(timeout).UnixNano(),
		index: 0,
	}
	c.inFlightPQ.Push(flightMsg)
	c.inFlightMessages[msg.ID] = flightMsg
	c.inFlightMutex.Unlock()
	return
}

func (c *RedisQueueConsumer) removeFromInFlightPQ(msgId MessageID) {
	c.inFlightMutex.Lock()
	item, ok := c.inFlightMessages[msgId]
	if !ok {
		c.inFlightMutex.Unlock()
		return
	}
	delete(c.inFlightMessages, msgId)
	if item.index == -1 {
		c.inFlightMutex.Unlock()
		return
	}
	c.inFlightPQ.Remove(item.index)
	c.inFlightMutex.Unlock()
	return
}

func (c *RedisQueueConsumer) requeueMessage(msgId MessageID, timeout time.Duration) error {
	conn := c.pool.Get()
	defer conn.Close()
	key := fmt.Sprintf("%s_%s", c.topic, msgId)
	body, err := rd.Bytes(conn.Do("GET", key))
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	_, err = conn.Do("LPUSH", c.topic, body)
	if err != nil {
		return err
	}
	conn.Do("DEL", key)
	msg := &Message{
		ID:   msgId,
		Body: body,
	}
	c.addToInFlightPQ(msg, timeout)
	return nil
}

func (c *RedisQueueConsumer) queueConsumerWorker() {
	for {
		message, err := c.getMessage(true)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if message == nil {
			//未取到消息
			continue
		}
		select {
		case c.MessageChan <- message:
			c.addToInFlightPQ(message, 30*time.Second)
		case <-c.exitChan:
			goto exit
		}
	}
exit:
}

func (c *RedisQueueConsumer) queueScanWorker() {
	workerTicker := time.NewTicker(time.Second)
	for {
		select {
		case <-workerTicker.C:
			c.processInFlightQueue(time.Now().UnixNano())
		case <-c.exitChan:
			goto exit
		}
	}
exit:
}

func (c *RedisQueueConsumer) processInFlightQueue(t int64) error {
	var err error
	for {
		c.inFlightMutex.Lock()
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()

		if msg == nil {
			goto exit
		}
		c.removeFromInFlightPQ(msg.ID)
		err = c.requeueMessage(msg.ID, 30*time.Second)
		if err != nil {
			goto exit
		}
	}

exit:
	return err
}
