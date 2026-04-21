package mydumper

import (
	"time"

	log "github.com/liusl104/go-mydumper/src/logrus"
)

type GAsyncQueue struct {
	queue  chan any
	length int64
	state  uint
	name   string
}

// try_pop removes and returns one item if the channel has a value; otherwise returns nil without blocking.
func (a *GAsyncQueue) try_pop() any {
	select {
	case task, ok := <-a.queue:
		if !ok {
			return nil
		}
		return task
	default:
		return nil
	}
}

// G_async_queue_timeout_pop pops an item from the queue or returns nil after timeout microseconds.
func G_async_queue_timeout_pop(a *GAsyncQueue, timeout uint64) any {
	return a.timeout_pop(timeout)
}

// timeout_pop blocks until an item is available or timeout (microseconds) expires; returns nil on timeout.
func (a *GAsyncQueue) timeout_pop(timeout uint64) any {
	timer := time.NewTimer(time.Duration(timeout) * time.Microsecond)
	defer timer.Stop()
	select {
	case task, ok := <-a.queue:
		if !ok {
			return nil
		}
		return task
	case <-timer.C:
		return nil
	}
}

// G_async_queue_unref drains the queue and closes the channel so any
// goroutine blocked in G_async_queue_pop will receive the zero value and return.
func G_async_queue_unref(a *GAsyncQueue) {
	if a == nil {
		return
	}
	a.unref()
}

// unref drains remaining items then closes the channel.
func (a *GAsyncQueue) unref() {
	for len(a.queue) > 0 {
		<-a.queue
	}
	defer func() {
		recover() // ignore double-close panic
	}()
	close(a.queue)
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

// G_async_queue_push pushes task onto the queue. Recovers from panic if the
// channel has been closed by G_async_queue_unref.
func G_async_queue_push(a *GAsyncQueue, task any) {
	defer func() {
		recover() // send on closed channel
	}()
	a.queue <- task
}

// G_async_queue_try_pop returns an item from the queue without blocking, or nil if empty.
func G_async_queue_try_pop(a *GAsyncQueue) any {
	log.Debugf("call try pop task [%s]", a.name)
	return a.try_pop()
}

// G_async_queue_pop blocks until an item is available, then removes and returns it.
// Returns nil if the channel has been closed (queue destroyed).
func G_async_queue_pop(a *GAsyncQueue) any {
	task, ok := <-a.queue
	if !ok {
		return nil
	}
	log.Debugf("call pop task [%s]", a.name)
	return task
}

// G_async_queue_length returns the number of buffered items in the channel (same as len(chan) for buffered queues).
// Using the atomic length field was unsafe: G_async_queue_pop decrements length after receive, so another goroutine
// could observe length>0 while the channel was already drained and trip false G_asserts in myloader.
func G_async_queue_length(a *GAsyncQueue) int64 {
	return int64(len(a.queue))
}
