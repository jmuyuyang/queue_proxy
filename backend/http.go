package backend

import (
	"bytes"
	"github.com/jmuyuyang/queue_proxy/config"
	"github.com/jmuyuyang/queue_proxy/util"
)

type HttpQueueProducer struct {
	sendUrl string
}

type HttpBatchQueueProducer struct {
	sendUrl string
}

/*
新建http发送器
*/
func NewHttpQueueProducer(config config.QueueAttrConfig) *HttpQueueProducer {
	if sendUrl, ok := config.Attr["send_url"].(string); ok {
		sendUrl += "?gzip=1"
		return &HttpQueueProducer{sendUrl: sendUrl}
	}
	return nil
}

/*
新建http batch发送器
*/
func (h *HttpQueueProducer) StartBatchProducer() (BatchQueueProducer, error) {
	return &HttpBatchQueueProducer{sendUrl: h.sendUrl}, nil
}

func (h *HttpQueueProducer) SetTopic(topic string) {

}

func (h *HttpQueueProducer) GetTopic() string {
	return ""
}

/*
发送单个数据
*/
func (h *HttpQueueProducer) SendMessage(data []byte) error {
	sendData, err := util.GzipData(data)
	if err != nil {
		return err
	}
	return util.DoRequest(h.sendUrl, []byte(sendData))
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
	return util.DoRequest(h.sendUrl, []byte(sendData))
}

func (h *HttpBatchQueueProducer) Stop() error {
	return nil
}

func (h *HttpBatchQueueProducer) Topic() string {
	return ""
}
