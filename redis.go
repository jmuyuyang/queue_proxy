package queue

import (
	"sync/atomic"
	"time"

	rd "github.com/garyburd/redigo/redis"
)

const (
	DEFAULT_BUFFER_SIZE     = 10
	REDIS_POOL_IDLE_TIMEOUT = 60 * 10
	REDIS_CONN_READ_TIMEOUT = 30 //读超时10s,用于brpop操作
	REDIS_POOL_PING_TIMEOUT = 5  //PING超时
)

type RedisQueue struct {
	pool   *rd.Pool
	config RedisConfig
	topic  string
	enable bool
}

type RedisPipelineQueue struct {
	conn          rd.Conn
	topic         string
	bufferSize    int32
	curBufferSize int32
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

func NewRedisQueue(config RedisConfig) *RedisQueue {
	timeout := time.Duration(config.Timeout) * time.Second
	loggerQueue := &RedisQueue{
		pool:   createRedisQueuePool(config.Bind, timeout, REDIS_POOL_IDLE_TIMEOUT*time.Second, config.Size),
		config: config,
		topic:  config.Topic,
		enable: true,
	}
	return loggerQueue
}

func (q *RedisQueue) SetTopic(topic string) {
	q.topic = topic
}

func (q *RedisQueue) IsActive() bool {
	return q.enable
}

/**
* 检查队列是否正常启用
 */
func (q *RedisQueue) CheckQueue() bool {
	if !q.checkConn() {
		//链接非活动
		q.enable = false
		return false
	}
	if !q.checkQueueLen() {
		//队列长队超过限制
		q.enable = false
		return false
	}
	q.enable = true
	return true
}

/**
* 检查队列链接是否正常
 */
func (q *RedisQueue) checkConn() bool {
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
func (q *RedisQueue) checkQueueLen() bool {
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

func (q *RedisQueue) SendMessage(data []byte) error {
	conn := q.pool.Get()
	defer conn.Close()
	_, err := conn.Do("LPUSH", q.topic, string(data))
	return err
}

func (q *RedisQueue) GetMessage() ([]byte, error) {
	conn := q.pool.Get()
	defer conn.Close()
	return rd.Bytes(conn.Do("LPOP"))
}

/**
* 开启pipeline
 */
func (q *RedisQueue) StartPipeline() (PipelineQueueProducer, error) {
	conn := q.pool.Get()
	err := conn.Send("MULTI")
	if err != nil {
		return nil, err
	}
	pipelineQueue := &RedisPipelineQueue{
		conn:          conn,
		topic:         q.config.Topic,
		bufferSize:    int32(DEFAULT_BUFFER_SIZE),
		curBufferSize: int32(0),
	}
	return pipelineQueue, nil
}

func (q *RedisPipelineQueue) SendMessage(log []byte) error {
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
func (q *RedisPipelineQueue) Flush() error {
	if atomic.LoadInt32(&q.curBufferSize) >= int32(0) {
		err := q.conn.Send("EXEC")
		atomic.StoreInt32(&q.curBufferSize, 0)
		return err
	}
	return nil
}

func (q *RedisPipelineQueue) Close() error {
	defer q.conn.Close()
	return q.Flush()
}
