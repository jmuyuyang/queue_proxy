package queue

import (
	"errors"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/jolestar/go-commons-pool"
)

type KafkaPoolFactory struct {
	addr    string
	timeout time.Duration
}

type KafkaConfig struct {
	Bind    string `yaml:"bind"`
	Timeout int    `yaml:"timeout"`
	Topic   string `yaml:"topic"`
	Size    int    `yaml:"size"`
}

type KafkaQueue struct {
	pool   *pool.ObjectPool
	config KafkaConfig
	topic  string
	enable bool
}

type KafkaAsyncQueue struct {
	producer sarama.AsyncProducer
	topic    string
}

func (f *KafkaPoolFactory) MakeObject() (*pool.PooledObject, error) {
	cfg := sarama.NewConfig()
	cfg.Net.DialTimeout = f.timeout
	cfg.Net.WriteTimeout = f.timeout
	cfg.Producer.Return.Successes = true
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	brokerAddrs := strings.Split(f.addr, ",")
	if len(brokerAddrs) == 0 {
		return nil, errors.New("no kafka brokers")
	}
	producer, err := sarama.NewSyncProducer(brokerAddrs, cfg)
	return pool.NewPooledObject(producer), err
}

func (f *KafkaPoolFactory) DestroyObject(object *pool.PooledObject) error {
	producer := object.Object.(sarama.SyncProducer)
	return producer.Close()
}

func (f *KafkaPoolFactory) ValidateObject(object *pool.PooledObject) bool {
	//do validate
	return true
}

func (f *KafkaPoolFactory) ActivateObject(object *pool.PooledObject) error {
	return nil
}

func (f *KafkaPoolFactory) PassivateObject(object *pool.PooledObject) error {
	//do passivate
	return nil
}

func createKafkaQueuePool(config KafkaConfig) *pool.ObjectPool {
	timeout := time.Duration(config.Timeout) * time.Second
	poolFactory := &KafkaPoolFactory{
		addr:    config.Bind,
		timeout: timeout,
	}
	cfg := pool.NewDefaultPoolConfig()
	cfg.MaxIdle = config.Size
	cfg.MinEvictableIdleTimeMillis = 1000 * IDLE_TIMEOUT //10分钟空闲时间
	return pool.NewObjectPool(poolFactory, cfg)
}

func NewKafkaQueue(config KafkaConfig) *KafkaQueue {
	return &KafkaQueue{
		pool:   createKafkaQueuePool(config),
		config: config,
		topic:  config.Topic,
		enable: true,
	}
}

func (q *KafkaQueue) SetTopic(topic string) {
	q.topic = topic
}

func (q *KafkaQueue) CheckQueue() bool {
	producer, err := q.pool.BorrowObject()
	if err != nil {
		q.enable = false
		return false
	}
	defer q.pool.ReturnObject(producer)
	q.enable = true
	return true
}

func (q *KafkaQueue) SendMessage(log []byte) error {
	obj, err := q.pool.BorrowObject()
	if err != nil {
		return err
	}
	producer := obj.(sarama.SyncProducer)
	msg := &sarama.ProducerMessage{Topic: q.topic, Value: sarama.StringEncoder(string(log))}
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		//出错即关闭链接
		q.pool.InvalidateObject(obj)
		producer.Close()
		return err
	}
	defer q.pool.ReturnObject(producer)
	return nil
}

func (q *KafkaQueue) IsActive() bool {
	return q.enable
}

func (q *KafkaQueue) StartPipeline() (PipelineQueueProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	producer, err := sarama.NewAsyncProducer([]string{q.config.Bind}, cfg)
	if err != nil {
		return nil, err
	}
	return &KafkaAsyncQueue{
		producer: producer,
		topic:    q.config.Topic,
	}, nil
}

func (q *KafkaAsyncQueue) SendMessage(log []byte) error {
	select {
	case q.producer.Input() <- &sarama.ProducerMessage{Topic: q.topic, Value: sarama.StringEncoder(string(log))}:
		return nil
	case err := <-q.producer.Errors():
		return err
	}
}

func (q *KafkaAsyncQueue) Flush() error {
	return nil
}

func (q *KafkaAsyncQueue) Close() error {
	return q.producer.Close()
}
