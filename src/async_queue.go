package mydumper

import (
	"sync/atomic"
	"time"
)

type GAsyncQueue struct {
	queue  chan any
	length int64
	state  uint
}

func (a *GAsyncQueue) pop() any {
	atomic.AddInt64(&a.length, -1)
	task := <-a.queue
	return task
}

func (a *GAsyncQueue) push(task any) {
	a.queue <- task
	atomic.AddInt64(&a.length, 1)
	return
}

func (a *GAsyncQueue) try_pop() any {
	if a.length <= 0 {
		return nil
	}
	atomic.AddInt64(&a.length, -1)
	task := <-a.queue
	return task
}
func G_async_queue_timeout_pop(a *GAsyncQueue, timeout uint64) any {
	return a.timeout_pop(timeout)
}
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
func G_async_queue_unref(a *GAsyncQueue) {
	a.unref()
}

func (a *GAsyncQueue) unref() {
	for {
		if a.length > 0 {
			a.pop()
		} else {
			break
		}

	}

}
func G_async_queue_new(buffer uint) *GAsyncQueue {
	return &GAsyncQueue{
		queue:  make(chan any, buffer),
		length: 0,
		state:  0,
	}
}

func G_async_queue_push(a *GAsyncQueue, task any) {
	a.queue <- task
	atomic.AddInt64(&a.length, 1)
	return
}
func G_async_queue_try_pop(a *GAsyncQueue) any {
	if a.length <= 0 {
		return nil
	}
	atomic.AddInt64(&a.length, -1)
	task := <-a.queue
	return task
}

func G_async_queue_pop(a *GAsyncQueue) any {
	atomic.AddInt64(&a.length, -1)
	task := <-a.queue
	return task
}

func G_async_queue_length(a *GAsyncQueue) int64 {
	return a.length
}
