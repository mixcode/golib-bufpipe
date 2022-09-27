package bufpipe

import (
	"context"
	"io"
	"sync/atomic"
)

// A lock-free one-time notification channel that can be cancelled.
// NotifyCh internally uses a channel which is closed when notified.
type NotifyCh[T any] struct {
	Value     T
	Cancelled bool
	//pcReceive, pcSend unsafe.Pointer // pointer to the channel, to be
	ch                    chan any
	flagReceive, flagSend int32
}

// Create a notification channel.
func NewNotifyCh[T any]() *NotifyCh[T] {
	ch := make(chan any)
	return &NotifyCh[T]{
		ch: ch,
	}
}

// Get a one-time channel for detecting notification, usually to mux with select{}.
// The returning channel will be closed when notificaion is sent or cancelled.
// The function returns nil if the channel is already fetched or notified.
// If the user does NOT consumed the returned channel for some reason, then the channel should be reverted to unreferenced state using UnfetchChannel().
func (c *NotifyCh[T]) FetchChannel() chan any {
	if c.flagReceive == 0 {
		if atomic.CompareAndSwapInt32(&c.flagReceive, 0, 1) {
			return c.ch
		}
	}
	return nil
}

// Revert the channel fetched by FetchChannel() to unused state.
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

// Cancel the notification channel setting the value.
// Returns false if the channel is already notified or cancelled.
func (c *NotifyCh[T]) CancelWithValue(value T) bool {
	ch := c.fetchSendChannel()
	if ch != nil {
		c.Value, c.Cancelled = value, true
		close(ch)
		return true
	}
	return false
}

// Wait for the notification.
// This function blocks until a Notify() or Cancel() call, or ctx.Done() is done.
// Returned value is the value set by Notify() or CancelWithValue().
// The returned err is nil if the notification is sent by Notify().
// The err is io.ErrClosedPipe if cancelled, and will be io.EOF if already used.
// If the functions terminated by the provided context, then err is the error of the context, or context.Cancelled.
func (c *NotifyCh[T]) Wait(ctx context.Context) (value T, err error) {
	ch := c.FetchChannel()
	if ch == nil {
		// alread closed or disabled
		return c.Value, io.EOF
	}
	select {

	case <-ch:
		value = c.Value
		if c.Cancelled {
			err = io.ErrClosedPipe
		} else {
			err = nil
		}
		return

	case <-ctx.Done():
		err = ctx.Err()
		if err == nil {
			err = context.Canceled
		}
		c.UnfetchChannel(ch)
		return
	}
}
