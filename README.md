## queue_proxy

- 支持redis/kafka/aliyun mns多种消息服务引擎,
- 统一发送/消费方式,接口简易(http restful/tcp)
- 瞬时切换后端消息服务(灾备切换)
- 多服务智能负载均衡调度

### producer端
- 本地disk queue做消息服务灾备,避免消息丢失,并支持数据压缩
- 队列级别限流控制及合理内存检测, 避免后端消息服务过载

### consumer端
- 屏蔽后端不同消息服务引擎消费方式的差异化
- 动态调整实际消费worker,低峰期避免空跑
- 针对redis queue增加message ack功能
- 增加delay queue支持(redis/kafka)

### 安装使用
```
  go get github.com/jmuyuyang/queue_proxy
```

### 配置说明
```
  queue:
    - 
      name: "hlg-kafka"
      type: kafka
      attr:
        bind: 172.16.2.216:9092
        timeout: 3
        pool_size: 5
    -
      name: "hlg-redis"
      type: redis
      attr:
        bind: 127.0.0.1:6379
        timeout: 3
        pool_size: 5
  disk:
    path: "./data"
    flush_timeout: 2
    compress_type: "gzip" //文件压缩方式
```

### 使用说明
```
    import queue "github.com/jmuyuyang/queue_proxy"
    val config queue.QueueConfig
    config = queue.ParseConfigFile(cfgFile)
    queue.NewQueueProducer(config)
	queue.InitQueue("hlg-redis","logcenter")
    queue.Start()

    queue.SendMessage(dateByte)
    queue.SetRateLimit(ratePerSecond) //限制限流(每秒流速)
    queue.Stop()
	
    queue.NewQueueConsumer(config)
    queue.InitQueue("hlg-kafka","logcenter")
    queue.Start()
    msg := <-queue.GetMessageChan()
```
