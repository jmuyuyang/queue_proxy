package ali_mns

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"context"

	"github.com/gogap/errors"
)

const (
	version = "2015-06-06"
)

const (
	DefaultTimeout int64 = 35
)

type Method string

var (
	errMapping map[string]errors.ErrCodeTemplate
)

func init() {
	initMNSErrors()
}

const (
	GET    Method = "GET"
	PUT           = "PUT"
	POST          = "POST"
	DELETE        = "DELETE"
)

type MNSClient interface {
	Send(method Method, headers map[string]string, message interface{}, resource string) (resp *http.Response, err error)
	SetProxy(url string)
	SetTimeout(timeout int)
}

type AliMNSClient struct {
	timeout     time.Duration
	url         string
	credential  Credential
	accessKeyId string
	client      *http.Client
	proxyURL    string

	clientLocker sync.Mutex
}

func NewAliMNSClient(url, accessKeyId, accessKeySecret string) MNSClient {
	if url == "" {
		panic("ali-mns: message queue url is empty")
	}

	credential := NewAliMNSCredential(accessKeySecret)

	aliMNSClient := new(AliMNSClient)
	aliMNSClient.credential = credential
	aliMNSClient.accessKeyId = accessKeyId
	aliMNSClient.url = url

	if globalurl := os.Getenv(GLOBAL_PROXY); globalurl != "" {
		aliMNSClient.proxyURL = globalurl
	}

	aliMNSClient.initClient()

	return aliMNSClient
}

func (p *AliMNSClient) SetProxy(url string) {
	if url == p.proxyURL {
		return
	}

	p.proxyURL = url
}

func (p *AliMNSClient) SetTimeout(timeout int) {
	p.timeout = time.Duration(timeout) * time.Second
}

func (p *AliMNSClient) initClient() {

	p.clientLocker.Lock()
	defer p.clientLocker.Unlock()

	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout: 3 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 3 * time.Second,
	}

	p.client = &http.Client{Transport: transport}
}

func (p *AliMNSClient) proxy(req *http.Request) (*url.URL, error) {
	if p.proxyURL != "" {
		return url.Parse(p.proxyURL)
	}
	return nil, nil
}

func (p *AliMNSClient) authorization(method Method, headers map[string]string, resource string) (authHeader string, err error) {
	if signature, e := p.credential.Signature(method, headers, resource); e != nil {
		return "", e
	} else {
		authHeader = fmt.Sprintf("MNS %s:%s", p.accessKeyId, signature)
	}

	return
}

func (p *AliMNSClient) Send(method Method, headers map[string]string, message interface{}, resource string) (resp *http.Response, err error) {
	var xmlContent []byte

	if message == nil {
		xmlContent = []byte{}
	} else {
		switch m := message.(type) {
		case []byte:
			{
				xmlContent = m
			}
		default:
			if bXml, e := xml.Marshal(message); e != nil {
				err = ERR_MARSHAL_MESSAGE_FAILED.New(errors.Params{"err": e})
				return
			} else {
				xmlContent = bXml
			}
		}
	}

	xmlMD5 := md5.Sum(xmlContent)
	strMd5 := fmt.Sprintf("%x", xmlMD5)

	if headers == nil {
		headers = make(map[string]string)
	}

	headers[MQ_VERSION] = version
	headers[CONTENT_TYPE] = "application/xml"
	headers[CONTENT_MD5] = base64.StdEncoding.EncodeToString([]byte(strMd5))
	headers[DATE] = time.Now().UTC().Format(http.TimeFormat)

	if authHeader, e := p.authorization(method, headers, fmt.Sprintf("/%s", resource)); e != nil {
		err = ERR_GENERAL_AUTH_HEADER_FAILED.New(errors.Params{"err": e})
		return
	} else {
		headers[AUTHORIZATION] = authHeader
	}

	url := p.url + "/" + resource

	postBodyReader := strings.NewReader(string(xmlContent))

	var req *http.Request
	if req, err = http.NewRequest(string(method), url, postBodyReader); err != nil {
		err = ERR_CREATE_NEW_REQUEST_FAILED.New(errors.Params{"err": err})
		return
	}

	for header, value := range headers {
		req.Header.Add(header, value)
	}

	if resp, err = p.client.Do(req); err != nil {
		err = ERR_SEND_REQUEST_FAILED.New(errors.Params{"err": err})
		return
	}

	return
}

func (p *AliMNSClient) ctx() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), p.timeout)
	return ctx
}

func initMNSErrors() {
	errMapping = map[string]errors.ErrCodeTemplate{
		"AccessDenied":               ERR_MNS_ACCESS_DENIED,
		"InvalidAccessKeyId":         ERR_MNS_INVALID_ACCESS_KEY_ID,
		"InternalError":              ERR_MNS_INTERNAL_ERROR,
		"InvalidAuthorizationHeader": ERR_MNS_INVALID_AUTHORIZATION_HEADER,
		"InvalidDateHeader":          ERR_MNS_INVALID_DATE_HEADER,
		"InvalidArgument":            ERR_MNS_INVALID_ARGUMENT,
		"InvalidDegist":              ERR_MNS_INVALID_DEGIST,
		"InvalidRequestURL":          ERR_MNS_INVALID_REQUEST_URL,
		"InvalidQueryString":         ERR_MNS_INVALID_QUERY_STRING,
		"MalformedXML":               ERR_MNS_MALFORMED_XML,
		"MissingAuthorizationHeader": ERR_MNS_MISSING_AUTHORIZATION_HEADER,
		"MissingDateHeader":          ERR_MNS_MISSING_DATE_HEADER,
		"MissingVersionHeader":       ERR_MNS_MISSING_VERSION_HEADER,
		"MissingReceiptHandle":       ERR_MNS_MISSING_RECEIPT_HANDLE,
		"MissingVisibilityTimeout":   ERR_MNS_MISSING_VISIBILITY_TIMEOUT,
		"MessageNotExist":            ERR_MNS_MESSAGE_NOT_EXIST,
		"QueueDeletedRecently":       ERR_MNS_QUEUE_DELETED_RECENTLY,
		"InvalidQueueName":           ERR_MNS_INVALID_QUEUE_NAME,
		"QueueNameLengthError":       ERR_MNS_QUEUE_NAME_LENGTH_ERROR,
		"QueueNotExist":              ERR_MNS_QUEUE_NOT_EXIST,
		"ReceiptHandleError":         ERR_MNS_RECEIPT_HANDLE_ERROR,
		"SignatureDoesNotMatch":      ERR_MNS_SIGNATURE_DOES_NOT_MATCH,
		"TimeExpired":                ERR_MNS_TIME_EXPIRED,
		"QpsLimitExceeded":           ERR_MNS_QPS_LIMIT_EXCEEDED,
	}
}

func ParseError(resp ErrorMessageResponse, resource string) (err error) {
	if errCodeTemplate, exist := errMapping[resp.Code]; exist {
		err = errCodeTemplate.New(errors.Params{"resp": resp, "resource": resource})
	} else {
		err = ERR_MNS_UNKNOWN_CODE.New(errors.Params{"resp": resp, "resource": resource})
	}
	return
}
