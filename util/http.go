package util

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"
)

var HTTP_BAD_STATUS = "Bad http status"
var DefaultTransport *http.Transport = &http.Transport{
	Dial: (&net.Dialer{
		Timeout:   20 * time.Second,
		KeepAlive: 30 * time.Second,
	}).Dial,
}

/*
http 发送
*/

func DoRequest(url string, body []byte) error {
	var resp *http.Response
	data := bytes.NewBuffer(body)
	req, _ := http.NewRequest("POST", url, data)
	client := &http.Client{Transport: DefaultTransport}
	resp, err := client.Do(req)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Request failed to url: %s\n", url)
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "Http status is %d\n", resp.StatusCode)
		return errors.New(HTTP_BAD_STATUS)
	}
	return nil
}

func GzipData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)

	_, err := zw.Write(data)
	if err != nil {
		zw.Close()
		return nil, err
	}
	zw.Close()

	return buf.Bytes(), nil
}
