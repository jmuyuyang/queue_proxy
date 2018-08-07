package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

const (
	TYPE_REDIS = "redis"
	TYPE_KAFKA = "kafka"
	TYPE_MNS   = "mns"
)

type Config struct {
	QueueConfig []QueueConfig `yaml:"queue"`
	DiskConfig  DiskConfig    `yaml:"disk"`
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
	FlushTimeout int    `yaml:"flush_timeout"`
	CompressType string `yaml:"compress_type"`
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
