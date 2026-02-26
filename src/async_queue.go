package mydumper

import (
	"sync/atomic"
	"time"

	log "github.com/liusl104/go-mydumper/src/logrus"
)

type GAsyncQueue struct {
	queue  chan any
	length int64
	state  uint
	name   string
}

// pop removes and returns one item from the queue; decrements length. Blocks until an item is available.
func (a *GAsyncQueue) pop() any {
	atomic.AddInt64(&a.length, -1)
	task := <-a.queue
	return task
}

// push adds task to the queue and increments length.
func (a *GAsyncQueue) push(task any) {
	a.queue <- task
	atomic.AddInt64(&a.length, 1)
}

// try_pop removes and returns one item if length > 0; otherwise returns nil without blocking.
func (a *GAsyncQueue) try_pop() any {
	if a.length <= 0 {
		return nil
	}
	task := <-a.queue
	atomic.AddInt64(&a.length, -1)
	return task
}

// G_async_queue_timeout_pop pops an item from the queue or returns nil after timeout microseconds.
func G_async_queue_timeout_pop(a *GAsyncQueue, timeout uint64) any {
	return a.timeout_pop(timeout)
}

// timeout_pop blocks until an item is available or timeout (microseconds) expires; returns nil on timeout.
func (a *GAsyncQueue) timeout_pop(timeout uint64) any {
	for {
		select {
		case task := <-a.queue:
			return task
		case <-time.After(time.Duration(timeout) * time.Microsecond):
			return nil
		}

	}
}

// G_async_queue_unref drains the queue (pops all remaining items).
func G_async_queue_unref(a *GAsyncQueue) {
	a.unref()
}

// unref drains the queue by popping until empty.
func (a *GAsyncQueue) unref() {
	for {
		if a.length > 0 {
			a.pop()
		} else {
			break
		}

	}
}

// G_async_queue_new creates a new async queue with capacity BufferSize. name is used for debug/tracing.
func G_async_queue_new(name string) *GAsyncQueue {
	return &GAsyncQueue{
		queue:  make(chan any, BufferSize),
		length: 0,
		state:  0,
		name:   name,
	}
}

// G_async_queue_push pushes task onto the queue and increments length.
func G_async_queue_push(a *GAsyncQueue, task any) {
	a.queue <- task
	atomic.AddInt64(&a.length, 1)
}

// G_async_queue_try_pop returns an item from the queue without blocking, or nil if empty.
func G_async_queue_try_pop(a *GAsyncQueue) any {
	log.Debugf("call try pop task [%s]", a.name)
	return a.try_pop()
}

// G_async_queue_pop blocks until an item is available, then removes and returns it and decrements length.
func G_async_queue_pop(a *GAsyncQueue) any {
	task := <-a.queue
	atomic.AddInt64(&a.length, -1)
	log.Debugf("call pop task [%s]", a.name)
	return task

}

// G_async_queue_length returns the current number of items in the queue (may be stale due to concurrency).
func G_async_queue_length(a *GAsyncQueue) int64 {
	return a.length
}
