package queue

import (
	"sync"
	"sync/atomic"
	"time"
)

// BoundedQueue implements a producer-consumer exchange similar to a ring buffer queue,
// where the queue is bounded and if it fills up due to slow consumers, the new items written by
// the producer force the earliest items to be dropped. The implementation is actually based on
// channels, with a special Reaper goroutine that wakes up when the queue is full and consumers
// the items from the top of the queue until its size drops back to maxSize
type BoundedQueue struct {
	capacity      int
	size          int32
	onDroppedItem func(item interface{})
	items         chan interface{}
	itemBuffer    []interface{}
	stopCh        chan struct{}
	stopWG        sync.WaitGroup
	stopped       int32
}

type BoundedQueueWorker interface {
	Consume(item interface{})
	IdleCheck()
}

// NewBoundedQueue constructs the new queue of specified capacity, and with an optional
// callback for dropped items (e.g. useful to emit metrics).
func NewBoundedQueue(capacity int, onDroppedItem func(item interface{})) *BoundedQueue {
	return &BoundedQueue{
		capacity:      capacity,
		onDroppedItem: onDroppedItem,
		items:         make(chan interface{}, capacity),
		stopCh:        make(chan struct{}),
	}
}

func (q *BoundedQueue) AddConsumeWorker(worker BoundedQueueWorker) {
	q.stopWG.Add(1)
	go func() {
		defer q.stopWG.Done()
		timer := time.NewTicker(time.Second)
		for {
			select {
			case item := <-q.items:
				atomic.AddInt32(&q.size, -1)
				worker.Consume(item)
			case <-timer.C:
				worker.IdleCheck()
			case <-q.stopCh:
				timer.Stop()
				return
			}
		}
	}()
}

// Produce is used by the producer to submit new item to the queue. Returns false in case of queue overflow.
func (q *BoundedQueue) Produce(item interface{}, timeout time.Duration) bool {
	if atomic.LoadInt32(&q.stopped) != 0 {
		q.onDroppedItem(item)
		return false
	}
	select {
	case q.items <- item:
		atomic.AddInt32(&q.size, 1)
		return true
	case <-time.After(timeout):
		if q.onDroppedItem != nil {
			q.onDroppedItem(item)
		}
		return false
	}
}

/**
* 停止消费worker
 */
func (q *BoundedQueue) StopConsumeWorker() {
	q.stopCh <- struct{}{}
	q.stopWG.Wait()
}

// Stop stops all consumers, as well as the length reporter if started,
// and releases the items channel. It blocks until all consumers have stopped.
func (q *BoundedQueue) Stop() {
	atomic.StoreInt32(&q.stopped, 1)
	close(q.stopCh)
	q.stopWG.Wait()
	close(q.items)
	if q.onDroppedItem != nil {
		for item := range q.items {
			q.onDroppedItem(item)
		}
	}
}

// Size returns the current size of the queue
func (q *BoundedQueue) Size() int {
	return int(atomic.LoadInt32(&q.size))
}

// Capacity returns capacity of the queue
func (q *BoundedQueue) Capacity() int {
	return q.capacity
}
