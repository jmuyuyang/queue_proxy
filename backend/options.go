package backend

import (
	"crypto/md5"
	"hash/crc32"
	"io"
	"os"
)

type Options struct {
	ID                   int64
	MemQueueSize         int     `yaml:"mem_queue_size"`
	MinWorkerNum         int     `yaml:"min_worker_num"`
	QueueInActiveTimeout int     `yaml:"queue_inactive_timeout`
	WorkerAdjustRatio    int     `yaml:"worker_adjust_ratio`
	QueueDelayRatio      float64 `yaml:"queue_delay_ratio"`
}

func NewOptions() *Options {
	hostname, _ := os.Hostname()

	h := md5.New()
	io.WriteString(h, hostname)
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Options{
		ID:                   defaultID,
		MemQueueSize:         10,
		MinWorkerNum:         1,
		QueueInActiveTimeout: 600,
		WorkerAdjustRatio:    2.0, //worker size调节倍率
		QueueDelayRatio:      10.0,
	}
}
