package pipe

import (
	"sync/atomic"
	"unsafe"
)

// A lock-free, first-in-first-out queue.
// https://www.cs.rochester.edu/u/scott/papers/1996_PODC_queues.pdf
type Queue[T any] struct {
	head, tail unsafe.Pointer
	size       int64
}

// Create a queue of type T
func NewQueue[T any]() *Queue[T] {
	h := &queueNode[T]{next: nil}
	return &Queue[T]{
		head: unsafe.Pointer(h),
		tail: unsafe.Pointer(h),
		size: 0,
	}
}

// Add a entry to the queue. Returns the size of entries in the queue.
// This is the implementation exactly on the paper.
func (q *Queue[T]) Enqueue(v T) int {
	pNew := unsafe.Pointer(&queueNode[T]{next: nil, value: v})
	for {
		pTail := q.tail
		tail := (*queueNode[T])(pTail)
		pNext := tail.next
		if pTail == q.tail { // tail is still there
			if pNext == nil {
				// Add entry to the last node
				if atomic.CompareAndSwapPointer(&tail.next, nil, pNew) {
					atomic.CompareAndSwapPointer(&q.tail, pTail, pNew) // note that q.tail could be changed on Dequeue()
					return int(atomic.AddInt64(&q.size, 1))
				}
			} else {
				atomic.CompareAndSwapPointer(&q.tail, pTail, pNext)
			}
		}
	}
	// no return
}

// Get a entry from the queue. Fetches the size of entires in the queue.
func (q *Queue[T]) Dequeue() (value T, ok bool) {
	for {
		pHead, pTail := q.head, q.tail
		head := (*queueNode[T])(pHead)
		pNext := head.next
		if pHead == q.head {
			if pHead == pTail {
				if pNext == nil {
					// No value
					ok = false
					return
				}
				// try to advance the tail pointer
				atomic.CompareAndSwapPointer(&q.tail, pTail, pNext)
			} else {
				if atomic.CompareAndSwapPointer(&q.head, pHead, pNext) {
					// note that original paper reads the value before the CAS,
					// but in that case, the value could be changed from the default on the fail of CAS.
					// Believe in the Go's automatic memory management.
					value = (*queueNode[T])(pNext).value
					ok = true
					atomic.AddInt64(&q.size, -1)
					return
				}
			}
		}
	}
}

// Number of entries in the queue.
func (q *Queue[T]) Len() int {
	return int(q.size)
}

type queueNode[T any] struct {
	next  unsafe.Pointer
	value T
}
