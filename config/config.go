package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

const (
	TYPE_REDIS = "redis"
	TYPE_KAFKA = "kafka"
	TYPE_MNS   = "mns"
	TYPE_HTTP  = "http"
)

type Config struct {
	ChannelConfig ChannelConfig `yaml:"channel"`
	QueueConfig   []QueueConfig `yaml:"queue"`
	DiskConfig    DiskConfig    `yaml:"disk"`
}

type ChannelConfig struct {
	Size        int               `yaml:"queue_size"`
	WorkerNum   int               `yaml:"worker_num"`
	Transaction TransactionConfig `yaml:"transaction"`
}

type TransactionConfig struct {
	FtLogPath     string `yaml:"fault_log_path"`
	BatchLen      int    `yaml:"batch_len"`
	BatchDataSize int64  `yaml:"batch_data_size"`
	BatchInterval int    `yaml:"batch_interval"`
	CommitTimeout int    `yaml:"commit_timeout"`
	MaxRetry      int    `yaml:"max_retry"`
	FailSleep     int    `yaml:"fail_sleep"`
}

type QueueConfig struct {
	Name string          `yaml:"name"`
	Type string          `yaml:"type"`
	Attr QueueAttrConfig `yaml:"attr"`
}

type QueueAttrConfig struct {
	Bind     string                 `yaml:"bind"`
	Timeout  int                    `yaml:"timeout"`
	PoolSize int                    `yaml:"pool_size"`
	Attr     map[string]interface{} `yaml:",inline"`
}

type DiskConfig struct {
	Path         string `yaml:"path"`
	BufferSize   int    `yaml:"buffer_size"`
	FlushTimeout int    `yaml:"flush_timeout"`
	CompressType string `yaml:"compress_type"`
	MaxMsgSize   int64  `yaml:"max_msg_size"`
}

func (c Config) GetQueueConfig(queueName string) QueueConfig {
	for _, queueCfg := range c.QueueConfig {
		if queueCfg.Name == queueName {
			return queueCfg
		}
	}
	return QueueConfig{}
}

func (c Config) GetDiskConfig() DiskConfig {
	return c.DiskConfig
}

/**
* 解析queue config
 */
func ParseConfigFile(cfgFile string) (Config, error) {
	cfg := Config{}
	data, err := ioutil.ReadFile(cfgFile)
	if err != nil {
		return cfg, err
	}
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return cfg, err
	}
	return cfg, nil
}
