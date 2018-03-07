package ali_mns

import (
	"fmt"
	"os"
	"strings"
)

var (
	DefaultNumOfMessages int32 = 16
)

const (
	PROXY_PREFIX = "MNS_PROXY_"
	GLOBAL_PROXY = "MNS_GLOBAL_PROXY"
)

type AliMNSQueue interface {
	GetTopic() string
	SetTopic(string)
	GetClient() MNSClient
	SendMessage(message MessageSendRequest) (resp MessageSendResponse, err error)
	BatchSendMessage(batchMessage BatchMessageSendRequest) (resp BatchMessageSendResponse, err error)
	ReceiveMessage(waitseconds ...int64) (MessageReceiveResponse, error)
	BatchReceiveMessage(numOfMessages int32, waitseconds ...int64) (BatchMessageReceiveResponse, error)
	PeekMessage() (MessageReceiveResponse, error)
	BatchPeekMessage(numOfMessages int32) (BatchMessageReceiveResponse, error)
	DeleteMessage(receiptHandle string) (err error)
	BatchDeleteMessage(receiptHandles ...string) (err error)
	ChangeMessageVisibility(receiptHandle string, visibilityTimeout int64) (resp MessageVisibilityChangeResponse, err error)
}

type MNSQueue struct {
	topic   string
	client  MNSClient
	decoder MNSDecoder
}

func NewMNSQueue(client MNSClient) AliMNSQueue {
	queue := new(MNSQueue)
	queue.client = client
	queue.decoder = NewAliMNSDecoder()

	proxyURL := ""
	queueProxyEnvKey := PROXY_PREFIX + strings.Replace(strings.ToUpper("mns_queue"), "-", "_", -1)
	if url := os.Getenv(queueProxyEnvKey); url != "" {
		proxyURL = url
	}

	client.SetProxy(proxyURL)

	return queue
}

func (p *MNSQueue) GetClient() MNSClient {
	return p.client
}

func (p *MNSQueue) SetTopic(topic string) {
	p.topic = topic
}

func (p *MNSQueue) GetTopic() string {
	return p.topic
}

func (p *MNSQueue) SendMessage(message MessageSendRequest) (resp MessageSendResponse, err error) {
	_, err = send(p.client, p.decoder, POST, nil, message, fmt.Sprintf("queues/%s/%s", p.topic, "messages"), &resp)
	return
}

func (p *MNSQueue) BatchSendMessage(batchMessage BatchMessageSendRequest) (resp BatchMessageSendResponse, err error) {
	_, err = send(p.client, p.decoder, POST, nil, batchMessage, fmt.Sprintf("queues/%s/%s", p.topic, "messages"), &resp)
	return
}

func (p *MNSQueue) ReceiveMessage(waitseconds ...int64) (MessageReceiveResponse, error) {
	resource := fmt.Sprintf("queues/%s/%s", p.topic, "messages")
	if waitseconds != nil && len(waitseconds) == 1 && waitseconds[0] >= 0 {
		resource = fmt.Sprintf("queues/%s/%s?waitseconds=%d", p.topic, "messages", waitseconds[0])
	}

	resp := MessageReceiveResponse{}
	_, err := send(p.client, p.decoder, GET, nil, nil, resource, &resp)
	return resp, err
}

func (p *MNSQueue) BatchReceiveMessage(numOfMessages int32, waitseconds ...int64) (BatchMessageReceiveResponse, error) {
	if numOfMessages <= 0 {
		numOfMessages = DefaultNumOfMessages
	}

	resource := fmt.Sprintf("queues/%s/%s?numOfMessages=%d", p.topic, "messages", numOfMessages)
	if waitseconds != nil && len(waitseconds) == 1 && waitseconds[0] >= 0 {
		resource = fmt.Sprintf("queues/%s/%s?numOfMessages=%d&waitseconds=%d", p.topic, "messages", numOfMessages, waitseconds[0])
	}

	resp := BatchMessageReceiveResponse{}
	_, err := send(p.client, p.decoder, GET, nil, nil, resource, &resp)
	return resp, err
}

func (p *MNSQueue) PeekMessage() (MessageReceiveResponse, error) {
	resource := fmt.Sprintf("queues/%s/%s?peekonly=true", p.topic, "messages")
	resp := MessageReceiveResponse{}
	_, err := send(p.client, p.decoder, GET, nil, nil, resource, &resp)
	return resp, err
}

func (p *MNSQueue) BatchPeekMessage(numOfMessages int32) (BatchMessageReceiveResponse, error) {
	if numOfMessages <= 0 {
		numOfMessages = DefaultNumOfMessages
	}

	resp := BatchMessageReceiveResponse{}
	_, err := send(p.client, p.decoder, GET, nil, nil, fmt.Sprintf("queues/%s/%s?numOfMessages=%d&peekonly=true", p.topic, "messages", numOfMessages), &resp)
	return resp, err
}

func (p *MNSQueue) DeleteMessage(receiptHandle string) (err error) {
	_, err = send(p.client, p.decoder, DELETE, nil, nil, fmt.Sprintf("queues/%s/%s?ReceiptHandle=%s", p.topic, "messages", receiptHandle), nil)
	return
}

func (p *MNSQueue) BatchDeleteMessage(receiptHandles ...string) (err error) {
	if receiptHandles == nil || len(receiptHandles) == 0 {
		return
	}

	handlers := ReceiptHandles{}

	for _, handler := range receiptHandles {
		handlers.ReceiptHandles = append(handlers.ReceiptHandles, handler)
	}

	_, err = send(p.client, p.decoder, DELETE, nil, handlers, fmt.Sprintf("queues/%s/%s", p.topic, "messages"), nil)
	return
}

func (p *MNSQueue) ChangeMessageVisibility(receiptHandle string, visibilityTimeout int64) (resp MessageVisibilityChangeResponse, err error) {
	_, err = send(p.client, p.decoder, PUT, nil, nil, fmt.Sprintf("queues/%s/%s?ReceiptHandle=%s&VisibilityTimeout=%d", p.topic, "messages", receiptHandle, visibilityTimeout), &resp)
	return
}
