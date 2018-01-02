package rateio

import (
	"sync"
	"sync/atomic"
	"time"
)

var Window = 50 * time.Millisecond

// Controller can limit multiple io.Reader(or io.Writer) within specific rate.
type Controller struct {
	capacity      int
	threshold     int
	cond          *sync.Cond
	done          chan struct{}
	ratePerSecond int
	closeOnce     *sync.Once
	enable        int32
}

func NewController(ratePerSecond int) *Controller {
	capacity := ratePerSecond * int(Window) / int(time.Second)
	self := &Controller{
		ratePerSecond: ratePerSecond,
		capacity:      capacity,
		cond:          sync.NewCond(new(sync.Mutex)),
		done:          make(chan struct{}, 1),
		closeOnce:     &sync.Once{},
		enable:        int32(0),
	}
	go self.run(capacity)
	return self
}

func (self *Controller) Start() {
	if atomic.LoadInt32(&self.enable) == 0 {
		self.closeOnce = &sync.Once{}
		go self.run(self.capacity)
	}
}

func (self *Controller) GetRateLimit() int {
	return int(self.capacity * int(time.Second) / int(Window))
}

func (self *Controller) SetRateLimit(ratePerSecond int) {
	if atomic.LoadInt32(&self.enable) == int32(1) {
		//开启状态先关闭
		self.Close()
	}
	self.closeOnce = &sync.Once{}
	capacity := ratePerSecond * int(Window) / int(time.Second)
	go self.run(capacity)
}

func (self *Controller) Assign(wait bool) bool {
	if atomic.LoadInt32(&self.enable) == 0 {
		return true
	}
	self.cond.L.Lock()
	for self.capacity == 0 {
		if wait {
			if atomic.LoadInt32(&self.enable) == int32(0) {
				return true
			}
			self.cond.Wait()
		} else {
			self.cond.L.Unlock()
			return false
		}
	}
	self.capacity -= 1
	self.cond.L.Unlock()
	return true
}

func (self *Controller) run(capacity int) {
	atomic.StoreInt32(&self.enable, int32(1))
	t := time.NewTicker(Window)
	for {
		select {
		case <-t.C:
			self.cond.L.Lock()
			self.capacity = capacity
			self.cond.L.Unlock()
			self.cond.Broadcast()
		case <-self.done:
			t.Stop()
			return
		}
	}
}

func (self *Controller) Close() {
	if atomic.LoadInt32(&self.enable) == int32(1) {
		self.closeOnce.Do(func() {
			self.done <- struct{}{}
			atomic.StoreInt32(&self.enable, int32(0))
		})
	}
}
