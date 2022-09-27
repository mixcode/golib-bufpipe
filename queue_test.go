package bufpipe

import (
	// "fmt"
	// "os"

	"sort"
	"sync"
	"testing"
	"time"
)

func BenchmarkQueue(b *testing.B) {
	benchQueue := func() {
		testCount := 1000
		queue := NewQueue[int]()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			for i := 0; i < testCount; i++ {
				queue.Enqueue(i)
			}
			wg.Done()
		}()
		go func() {
			for {
				n, ok := queue.Dequeue()
				if !ok {
					time.Sleep(10 * time.Nanosecond)
				}
				if n == testCount-1 {
					break
				}
			}
			wg.Done()
		}()
		wg.Wait()
	}

	for i := 0; i < b.N; i++ {
		benchQueue()
	}
}

func TestQueue(t *testing.T) {

	queue1 := NewQueue[int]()

	n, ok := queue1.Dequeue()
	if ok {
		t.Errorf("blank queue returned a data: %d", n)
	}
	testV := 100
	n = queue1.Enqueue(testV)
	if n != 1 {
		t.Errorf("queue size mismatch; expected 1, actual %d", n)
	}
	n, ok = queue1.Dequeue()
	if !ok || n != testV {
		t.Errorf("invalid dequeue; must return %d, actual %d", testV, n)
	}
	n = queue1.Len()
	if n != 0 {
		t.Errorf("queue size mismatch; expected 0, actual %d", n)
	}

	queue2 := NewQueue[int]()

	stopOut := false
	testCount := 10000 // 0.006 sec
	//testCount := 10000000 // 4 sec

	nIn, nOut := 7, 7
	var inWg, outWg sync.WaitGroup

	// drain threads
	counts := make([]int, nOut)
	outWg.Add(nOut)
	for i := 0; i < nOut; i++ {
		go func(k int) {
			defer outWg.Done()
			for {
				n, ok := queue1.Dequeue()
				if ok {
					counts[k]++
					queue2.Enqueue(n)
				}
				if !ok && stopOut {
					break
				}
			}
		}(i)
	}

	// source threads
	inWg.Add(nIn)
	inCnt := make([]int, nIn)
	for i := 0; i < nIn; i++ {
		go func(offset int) {
			defer inWg.Done()
			for i := offset; i < testCount; i += nIn {
				queue1.Enqueue(i)
				inCnt[offset]++
			}
		}(i)
	}
	inWg.Wait()
	//log.Printf("%v", inCnt)
	sum := 0
	for _, c := range inCnt {
		sum += c
	}
	if sum != testCount {
		t.Errorf("enqueued entry count not match; expected %d, actual %d", testCount, sum)
	}
	stopOut = true
	outWg.Wait()

	// count number of dequeued entries
	sum = 0
	for _, c := range counts {
		//if c == 0 {
		//	t.Errorf("goroutine %d processed no entries", i)
		//}
		sum += c
	}
	if sum != testCount {
		t.Errorf("dequeued entry count not match; expected %d, actual %d", testCount, sum)
	}

	// count number of enqueued entries
	if queue2.Len() != testCount {
		t.Errorf("enqueued entry count not match; expected %d, actual %d", testCount, queue2.Len())
	}
	out := make([]int, testCount)
	for i := 0; i < testCount; i++ {
		n, ok := queue2.Dequeue()
		if !ok {
			t.Fatalf("dequeue failed")
		}
		out[i] = n
	}
	sort.Ints(out)

	for i := 0; i < testCount; i++ {
		if out[i] != i {
			t.Fatalf("dequeued data incorrect; position %d, value %d", i, out[i])
		}
	}
}
