package pipe

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
)

var (
	ErrNoData = fmt.Errorf("no data") // no data is in FifoQueue
)

// pipe is a queue with Closer() and Receive().
// Please note that this object does NOT implmentes io.PipeReader and io.PipeWriter interface.
// Especially, Close() closes the stream on write side, but the data remains OK on the read side.
type Pipe[T any] struct {
	queue *Queue[T]

	readClosed, writeClosed bool
	writeCloseCh            chan any

	ch                chan *NotifyCh[any] // channel for notification object for Read()
	blockingReadCount int32               // number of concurrent Read() running
}

// Make a new pipe of type T.
func NewPipe[T any]() *Pipe[T] {
	return &Pipe[T]{
		queue:        NewQueue[T](),
		writeCloseCh: make(chan any),
		ch:           make(chan *NotifyCh[any], 16),
	}
}

// Number of data entries in the pipe.
func (q *Pipe[T]) Len() int {
	return q.queue.Len()
}

// Append a data to the pipe.
// n is current number of entries in the pipe.
// If the pipe is closed, an io.ErrClosedPipe is returned.
func (q *Pipe[T]) Append(v T) (n int, err error) {
	if q.writeClosed {
		err = io.ErrClosedPipe
		return
	}
	n = q.queue.Enqueue(v)

	for {
		var nc *NotifyCh[any]
		select {
		case nc = <-q.ch:
		default:
		}
		if nc == nil || nc.Notify(nil) {
			return
		}
	}
}

// Get a data from the pipe.
// if there is no data and the pipe is NOT closed, then returns ErrNoData.
// if there is no data and the pipe is closed, then returns io.EOF.
func (q *Pipe[T]) Fetch() (v T, err error) {
	if q.readClosed {
		err = io.EOF
		return
	}
	v, ok := q.queue.Dequeue()
	if ok {
		return
	}
	if q.writeClosed {
		q.readClosed = true
		err = io.EOF
	} else {
		err = ErrNoData
	}
	return
}

// Read a data from the pipe.
// This function blocks until a new data is received or the pipe is closed.
// Returns io.EOF if the pipe is closed and no data left.
func (q *Pipe[T]) Read(ctx context.Context) (p T, err error) {
	// Increase the waiting Read() count
	atomic.AddInt32(&q.blockingReadCount, 1)
	defer atomic.AddInt32(&q.blockingReadCount, -1)
	if q.readClosed {
		err = io.EOF
		return
	}

	for {
		p, err = q.Fetch()
		if err != ErrNoData {
			return
		}

		// register a notification channel
		ch := NewNotifyCh[any]()
		waitCh := ch.FetchChannel()
		q.ch <- ch

		select {
		case <-waitCh: // new data notification
			p, err = q.Fetch()
			if err != ErrNoData {
				return
			}
			// (err == ErrNoData) may means that
			// a data is added but already fetched by another goroutine.
			// wait again.

		case <-ctx.Done(): // context error
			ch.Cancel() // cancel the notification channel
			err = ctx.Err()
			if err == nil {
				err = context.Canceled
			}
			return

		case <-q.writeCloseCh: // the Pipe is closed
			// cancel the notification channel and read again
			ch.Cancel()
		}
	}
}

// Close the pipe on the write side.
// After the Close(), Append() will fail but Fetch() and Receive() do work until the data runs out.
func (q *Pipe[T]) Close() bool {
	if q.writeClosed {
		return false
	}
	q.writeClosed = true
	close(q.writeCloseCh)

	// kill waiting Receive()
	for q.blockingReadCount > 0 {
		select {
		case ch := <-q.ch:
			ch.Cancel()
		default:
		}
	}
	return true
}
