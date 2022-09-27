package pipe

import (
	"context"
	"io"
	"sync/atomic"
)

// A lock-free one-time notification channel that can be cancelled from the initialization side.
type NotifyCh[T any] struct {
	Value     T
	Cancelled bool
	//pcReceive, pcSend unsafe.Pointer // pointer to the channel, to be
	ch                    chan any
	flagReceive, flagSend int32
}

// Create a channel
func NewNotifyCh[T any]() *NotifyCh[T] {
	ch := make(chan any)
	return &NotifyCh[T]{
		ch: ch,
	}
}

// Get the notification channel.
// If the channel is already fetched or used, then returns nil.
// If the fetched channel is not used, then the channel may returned using UnfetchChannel().
func (c *NotifyCh[T]) FetchChannel() chan any {
	if c.flagReceive == 0 {
		if atomic.CompareAndSwapInt32(&c.flagReceive, 0, 1) {
			return c.ch
		}
	}
	return nil
}

// Return the channel fetched by FetchChannel().
// Do not return if the channel is notified / closed.
func (c *NotifyCh[T]) UnfetchChannel(ch chan any) bool {
	if ch != c.ch {
		// cannot return the different channel
		return false
	}
	return atomic.CompareAndSwapInt32(&c.flagReceive, 1, 0)
}

// fetch send-side channel
func (c *NotifyCh[T]) fetchSendChannel() chan any {
	if c.flagSend == 0 {
		if atomic.CompareAndSwapInt32(&c.flagSend, 0, 1) {
			return c.ch
		}
	}
	return nil
}

// Send notification to the notification channel.
// if the notification channel is already notified or cancelled, then returns false.
func (c *NotifyCh[T]) Notify(value T) bool {
	ch := c.fetchSendChannel()
	if ch == nil {
		return false
	}
	c.Value = value
	close(ch)
	return true
}

// Cancel the notification channel.
// Returns false if the channel is already notified or cancelled.
func (c *NotifyCh[T]) Cancel() bool {
	ch := c.fetchSendChannel()
	if ch != nil {
		c.Cancelled = true
		close(ch)
		return true
	}
	return false
}

// Wait for the notification channel.
// If the channel is notified, then the notification status is
func (c *NotifyCh[T]) Wait(ctx context.Context) (value T, err error) {
	ch := c.FetchChannel()
	if ch == nil {
		// alread closed or disabled
		return c.Value, io.ErrClosedPipe
	}
	select {

	case <-ch:
		return c.Value, nil

	case <-ctx.Done():
		err = ctx.Err()
		if err == nil {
			err = context.Canceled
		}
		c.UnfetchChannel(ch)
		return
	}
}
