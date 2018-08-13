package util

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
	transaction   int32
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

func (q *BoundedQueue) StartTransaction() {
	atomic.StoreInt32(&q.transaction, 1)
	q.itemBuffer = make([]interface{}, 0)
}

func (q *BoundedQueue) StartConsumers(num int, consumer func(item interface{})) {
	for i := 0; i < num; i++ {
		q.stopWG.Add(1)
		go func() {
			defer q.stopWG.Done()
			for {
				select {
				case item := <-q.items:
					atomic.AddInt32(&q.size, -1)
					if atomic.LoadInt32(&q.transaction) != 0 {
						q.itemBuffer = append(q.itemBuffer, item)
					}
					consumer(item)
				case <-q.stopCh:
					return
				}
			}
		}()
	}
}

func (q *BoundedQueue) Commit() {
	q.itemBuffer = make([]interface{}, 0)
}

func (q *BoundedQueue) Rollback() {
	for _, item := range itemBuffer {
		q.Produce(item)
	}
	q.commit()
}

// Produce is used by the producer to submit new item to the queue. Returns false in case of queue overflow.
func (q *BoundedQueue) Produce(item interface{}) bool {
	if atomic.LoadInt32(&q.stopped) != 0 {
		q.onDroppedItem(item)
		return false
	}
	select {
	case q.items <- item:
		atomic.AddInt32(&q.size, 1)
		return true
	default:
		if q.onDroppedItem != nil {
			q.onDroppedItem(item)
		}
		return false
	}
}

// Stop stops all consumers, as well as the length reporter if started,
// and releases the items channel. It blocks until all consumers have stopped.
func (q *BoundedQueue) Stop() {
	atomic.StoreInt32(&q.stopped, 1) // disable producer
	close(q.stopCh)
	q.stopWG.Wait()
	close(q.items)
}

func (q *BoundedQueue) TransactionBuffer() []interface{} {
	return q.itemBuffer
}

func (q *BoundedQueue) TransactionSize() int {
	return len(q.itemBuffer)
}

// Size returns the current size of the queue
func (q *BoundedQueue) Size() int {
	return int(atomic.LoadInt32(&q.size))
}

// Capacity returns capacity of the queue
func (q *BoundedQueue) Capacity() int {
	return q.capacity
}
