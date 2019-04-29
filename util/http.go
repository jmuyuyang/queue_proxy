package util

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"golang.org/x/net/context"
	"io/ioutil"
	"net"
	"net/http"
	"time"
)

var DefaultTransport *http.Transport = &http.Transport{
	Dial: (&net.Dialer{
		Timeout:   20 * time.Second,
		KeepAlive: 30 * time.Second,
	}).Dial,
}

/*
http 发送
*/
func DoRequest(url string,ctx context.Context, body *bytes.Buffer) error {
	req, _ := http.NewRequest("POST", url, body)
	req = req.WithContext(ctx)
	client := &http.Client{Transport: DefaultTransport}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("Request failed to url: %s err: %s", url, err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respContent, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("Http status is %d, content: %s", resp.StatusCode, string(respContent))
	}
	return nil
}

/*
gzip压缩
*/
func GzipData(data []byte) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	zw := gzip.NewWriter(buf)
	_, err := zw.Write(data)
	if err != nil {
		zw.Close()
		return nil, err
	}
	zw.Close()
	return buf, nil
}
