package main

import queue "github.com/jmuyuyang/queue_proxy"
import "time"

func main() {
	cfg, _ := queue.ParseConfigFile("./queue_proxy.yml")
	q := queue.NewQueueProducer(cfg)
	q.SetQueueAttr("hlg-redis","ttxsapp")
	q.Start()
	
	q.SetRateLimit(5)
	for i:=0;i<10;i++{
		go q.SendMessage([]byte("hello"))
	}
	
	time.Sleep(time.Second)
	q.Stop()
}
