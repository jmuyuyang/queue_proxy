package backend

import (
	"errors"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/jmuyuyang/queue_proxy/config"
	"github.com/jolestar/go-commons-pool"
)

const KAFKA_POOL_IDLE_TIMEOUT = 60 * 10

type KafkaPoolFactory struct {
	addr    string
	timeout time.Duration
}

type kafkaQueue struct {
	pool   *pool.ObjectPool
	topic  string
	config config.QueueAttrConfig
	active bool
}

type KafkaQueueProducer struct {
	kafkaQueue
}

type KafkaAsyncProducer struct {
	producer sarama.AsyncProducer
	topic    string
}

func (f *KafkaPoolFactory) MakeObject() (*pool.PooledObject, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_0_0_0
	cfg.Net.DialTimeout = f.timeout
	cfg.Net.WriteTimeout = f.timeout
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
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

func createKafkaQueuePool(config config.QueueAttrConfig) *pool.ObjectPool {
	timeout := time.Duration(config.Timeout) * time.Second
	poolFactory := &KafkaPoolFactory{
		addr:    config.Bind,
		timeout: timeout,
	}
	cfg := pool.NewDefaultPoolConfig()
	cfg.MaxIdle = config.PoolSize
	cfg.MinEvictableIdleTimeMillis = 1000 * KAFKA_POOL_IDLE_TIMEOUT //10分钟空闲时间
	return pool.NewObjectPool(poolFactory, cfg)
}

func newKafkaQueue(config config.QueueAttrConfig) kafkaQueue {
	return kafkaQueue{
		pool:   createKafkaQueuePool(config),
		config: config,
		active: true,
	}
}

func (q *kafkaQueue) SetTopic(topic string) {
	q.topic = topic
	if _, ok := q.config.Attr["auto_create"]; ok {
		autoCreate := q.config.Attr["auto_create"].(bool)
		if autoCreate {
			//自动创建topic
			q.createTopic()
		}
	}
}

func (q *kafkaQueue) GetTopic() string {
	return q.topic
}

/**
* 创建kafka topic
 */
func (q *kafkaQueue) createTopic() error {
	brokerAddrs := strings.Split(q.config.Bind, ",")
	broker := sarama.NewBroker(brokerAddrs[0])
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_0_0_0
	broker.Open(cfg)
	if ok, _ := broker.Connected(); !ok {
		return errors.New("connect to kafka server failed")
	}
	req := sarama.CreateTopicsRequest{
		TopicDetails: make(map[string]*sarama.TopicDetail),
	}
	var partitionNum int32 = 1
	if _, ok := q.config.Attr["partition_num"]; ok {
		partitionNum = int32(q.config.Attr["partition_num"].(int))
	}
	req.TopicDetails[q.topic] = &sarama.TopicDetail{
		//默认副本数均为1
		NumPartitions:     partitionNum,
		ReplicationFactor: 1,
	}
	res, err := broker.CreateTopics(&req)
	if err != nil {
		return err
	}
	if _, ok := res.TopicErrors[q.topic]; ok {
		topicErr := res.TopicErrors[q.topic]
		if topicErr.Err == sarama.ErrTopicAlreadyExists {
			return nil
		}
		return topicErr.Err
	}
	return nil
}

/**
* 检测队列是否活跃
 */
func (q *kafkaQueue) CheckActive() bool {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_0_0_0
	cfg.Net.DialTimeout = time.Duration(q.config.Timeout) * time.Second
	brokerAddrs := strings.Split(q.config.Bind, ",")
	tmpCli, err := sarama.NewClient(brokerAddrs, cfg)
	if err != nil {
		q.active = false
		q.pool.Clear()
		return false
	}
	q.active = true
	tmpCli.Close()
	return true
}

func (q *kafkaQueue) IsActive() bool {
	return q.active
}

func NewKafkaQueueProducer(config config.QueueAttrConfig) *KafkaQueueProducer {
	q := KafkaQueueProducer{
		kafkaQueue: newKafkaQueue(config),
	}
	return &q
}

func (q *KafkaQueueProducer) SendMessage(data []byte) error {
	obj, err := q.pool.BorrowObject()
	if err != nil {
		return err
	}
	producer := obj.(sarama.SyncProducer)
	msg := &sarama.ProducerMessage{Topic: q.topic, Value: sarama.StringEncoder(string(data))}
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

func (q *KafkaQueueProducer) StartPipeline() (PipelineQueueProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_0_0_0
	cfg.Producer.Return.Successes = true
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	producer, err := sarama.NewAsyncProducer([]string{q.config.Bind}, cfg)
	if err != nil {
		return nil, err
	}
	return &KafkaAsyncProducer{
		producer: producer,
		topic:    q.topic,
	}, nil
}

/*
* 停止kafka queue producer
 */
func (q *KafkaQueueProducer) Stop() error {
	q.pool.Close()
	return nil
}

func (q *KafkaAsyncProducer) SendMessage(log []byte) error {
	msg := &sarama.ProducerMessage{Topic: q.topic, Value: sarama.StringEncoder(string(log))}
	for {
		select {
		case q.producer.Input() <- msg:
			return nil
		case <-q.producer.Successes():
			//上一次的成功请求,本次发送依然要继续
			continue
		case err := <-q.producer.Errors():
			return err
		}
	}
}

func (q *KafkaAsyncProducer) Flush() error {
	return nil
}

func (q *KafkaAsyncProducer) Stop() error {
	return q.producer.Close()
}
