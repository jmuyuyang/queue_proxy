package backend

import (
	"bytes"
	"context"
	"github.com/jmuyuyang/queue_proxy/config"
	"github.com/jmuyuyang/queue_proxy/util"
	"time"
)

var defaultHttpTimeout = 15

type HttpQueueProducer struct {
	sendUrl string
	topic   string
	timeout time.Duration
}

type HttpBatchQueueProducer struct {
	sendUrl string
	topic   string
	timeout time.Duration
}

/*
新建http发送器
*/
func NewHttpQueueProducer(config config.QueueAttrConfig) *HttpQueueProducer {
	if config.Timeout == 0 {
		config.Timeout = defaultHttpTimeout
	}
	timeout := time.Duration(config.Timeout) * time.Second
	if sendUrl, ok := config.Attr["send_url"].(string); ok {
		sendUrl += "?gzip=1"
		return &HttpQueueProducer{sendUrl: sendUrl, timeout: timeout}
	}
	return nil
}

/*
新建http batch发送器
*/
func (h *HttpQueueProducer) StartBatchProducer() (BatchQueueProducer, error) {
	return &HttpBatchQueueProducer{sendUrl: h.sendUrl, topic: h.topic, timeout: h.timeout}, nil
}

func (h *HttpQueueProducer) SetTopic(topic string) {
	h.topic = topic
}

func (h *HttpQueueProducer) GetTopic() string {
	return h.topic
}

/*
发送单个数据
*/
func (h *HttpQueueProducer) SendMessage(data []byte) error {
	sendData, err := util.GzipData(data)
	if err != nil {
		return err
	}
	ctx, _ := context.WithTimeout(context.Background(), h.timeout)
	return util.DoRequest(h.sendUrl, ctx, sendData)
}

func (h *HttpQueueProducer) CheckActive() bool {
	return true
}

func (h *HttpQueueProducer) IsActive() bool {
	return true
}

func (h *HttpQueueProducer) Stop() error {
	return nil
}

/*
发送多个数据
是用换行符分割数据
*/
func (h *HttpBatchQueueProducer) SendMessages(datas [][]byte) error {
	var buf bytes.Buffer
	for i, data := range datas {
		buf.Write(data)
		if i < len(datas)-1 {
			buf.WriteString("\n")
		}
	}
	sendData, err := util.GzipData(buf.Bytes())
	if err != nil {
		return err
	}
	ctx, _ := context.WithTimeout(context.Background(), h.timeout)
	return util.DoRequest(h.sendUrl, ctx, sendData)
}

func (h *HttpBatchQueueProducer) Stop() error {
	return nil
}

func (h *HttpBatchQueueProducer) Topic() string {
	return h.topic
}
