package rateio

import (
	"sync"
	"sync/atomic"
	"time"
)

var Window = 50 * time.Millisecond

// Controller can limit multiple io.Reader(or io.Writer) within specific rate.
type Controller struct {
	capacity        int
	availableTokens int
	cond            *sync.Cond
	reset           chan int
	done            chan int
	enable          int32
}

func NewController(ratePerSecond int) *Controller {
	capacity := ratePerSecond * int(Window) / int(time.Second)
	self := &Controller{
		capacity:        capacity,
		availableTokens: capacity,
		cond:            sync.NewCond(new(sync.Mutex)),
		reset:           make(chan int),
		done:            make(chan int),
		enable:          int32(0),
	}
	return self
}

func (self *Controller) Start() {
	if atomic.LoadInt32(&self.enable) == 0 {
		go self.run()
		atomic.StoreInt32(&self.enable, int32(1))
	}
}

func (self *Controller) GetRateLimit() int {
	return int(self.capacity * int(time.Second) / int(Window))
}

/**
* 重置rate per second
 */
func (self *Controller) SetRateLimit(ratePerSecond int) {
	capacity := ratePerSecond * int(Window) / int(time.Second)
	self.reset <- capacity
}

func (self *Controller) Assign(wait bool) bool {
	if atomic.LoadInt32(&self.enable) == 0 {
		return true
	}
	self.cond.L.Lock()
	for self.availableTokens == 0 {
		if wait {
			self.cond.Wait()
		} else {
			self.cond.L.Unlock()
			return false
		}
	}
	self.availableTokens -= 1
	self.cond.L.Unlock()
	return true
}

func (self *Controller) run() {
	t := time.NewTicker(Window)
	for {
		select {
		case <-t.C:
			self.cond.L.Lock()
			self.availableTokens = self.capacity
			self.cond.L.Unlock()
			self.cond.Broadcast()
		case capacity := <-self.reset:
			self.cond.L.Lock()
			self.capacity = capacity
			self.cond.L.Unlock()
		case <-self.done:
			return
		}
	}
}

func (self *Controller) Stop() {
	if atomic.LoadInt32(&self.enable) == int32(1) {
		self.done <- 1
		atomic.StoreInt32(&self.enable, int32(0))
	}
}
