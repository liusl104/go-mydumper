package mydumper

import (
	"sync/atomic"
	"time"
)

type asyncQueue struct {
	queue  chan any
	length int64
	state  uint
}

func (a *asyncQueue) pop() any {
	atomic.AddInt64(&a.length, -1)
	task := <-a.queue
	return task
}

func (a *asyncQueue) push(task any) {
	a.queue <- task
	atomic.AddInt64(&a.length, 1)
	return
}

func (a *asyncQueue) try_pop() any {
	if a.length <= 0 {
		return nil
	}
	atomic.AddInt64(&a.length, -1)
	task := <-a.queue
	return task
}
func (a *asyncQueue) timeout_pop(timeout uint64) any {
	for {
		select {
		case task := <-a.queue:
			return task
		case <-time.After(time.Duration(timeout) * time.Microsecond):
			return nil
		}

	}

}

func (a *asyncQueue) unref() {
	for {
		if a.length > 0 {
			a.pop()
		} else {
			break
		}

	}

}
func g_async_queue_new(buffer int) *asyncQueue {
	return &asyncQueue{
		queue:  make(chan any, buffer),
		length: 0,
		state:  0,
	}
}
