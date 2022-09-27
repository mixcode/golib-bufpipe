package pipe

import (
	"context"
	"io"
	"sort"
	"sync"
	"testing"
	"time"
)

func BenchmarkPipe(b *testing.B) {

	benchFifo := func() {
		testCount := 1000
		queue := NewPipe[int]()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < testCount; i++ {
				queue.Append(i)
			}
			queue.Close()
		}()
		go func() {
			defer wg.Done()
			c, v := 0, 0
			for {
				n, err := queue.Fetch()
				if err == ErrNoData {
					time.Sleep(10 * time.Nanosecond)
					continue
				}
				if err == io.EOF {
					break
				}
				if err != nil {
					b.Error(err)
					break
				}
				v = n
				c++
			}
			if c != testCount || v != testCount-1 {
				b.Errorf("data count mismatch: %d/%d", v, c)
			}
		}()
		wg.Wait()
	}

	for i := 0; i < b.N; i++ {
		benchFifo()
	}
}

func TestPipe(t *testing.T) {

	var err error
	queue1 := NewPipe[int]()

	n, err := queue1.Fetch()
	if err != ErrNoData {
		t.Errorf("blank queue returned a data: %d", n)
	}
	testV := 100
	n, err = queue1.Append(testV)
	if err != nil {
		t.Error(err)
	}
	if n != 1 {
		t.Errorf("queue size mismatch; expected 1, actual %d", n)
	}
	n, err = queue1.Fetch()
	if err != nil || n != testV {
		t.Errorf("invalid dequeue; must return %d, actual %d (error %v)", testV, n, err)
	}
	n = queue1.Len()
	if n != 0 {
		t.Errorf("queue size mismatch; expected 0, actual %d", n)
	}

	// receive and close test
	var wg sync.WaitGroup
	rq := NewPipe[int]()
	wg.Add(1)
	go func() {
		defer wg.Done()
		c := 0
		for {
			n, err := rq.Receive(context.Background())
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Errorf("receive failed: %v", err)
				break
			}
			if n != c {
				t.Errorf("invalid receive data %d", n)
				break
			}
			c++
		}
		if c != testV {
			t.Errorf("invalid receive count %d", c)
		}
	}()
	for i := 0; i < testV; i++ {
		rq.Append(i)
	}
	rq.Close()
	wg.Wait()

	queue2 := NewPipe[int]()
	threshold := 10000

	nIn, nOut := 7, 7
	var inWg, outWg sync.WaitGroup

	// drain threads
	counts := make([]int, nOut)
	outWg.Add(nOut)
	for i := 0; i < nOut; i++ {
		if i%2 == 0 {
			// use Fetch()
			go func(k int) {
				defer outWg.Done()
				for {
					n, err := queue1.Fetch()
					if err == nil {
						counts[k]++
						queue2.Append(n)
						continue
					}
					if err == io.EOF {
						// no more data
						break
					}
					if err != ErrNoData {
						t.Errorf("drain thread %d error: %v", k, err)
						break
					}
					time.Sleep(1 * time.Nanosecond)
				}
			}(i)
		} else {
			// use Receive()
			go func(k int) {
				defer outWg.Done()
				for {
					n, err := queue1.Receive(context.Background())
					if err == io.EOF {
						// no more data
						break
					}
					counts[k]++
					queue2.Append(n)
				}
			}(i)
		}
	}

	// source threads
	inWg.Add(nIn)
	inCnt := make([]int, nIn)
	for i := 0; i < nIn; i++ {
		go func(k int) {
			defer inWg.Done()
			for i := k; i < threshold; i += nIn {
				queue1.Append(i)
				inCnt[k]++
			}
		}(i)
	}
	inWg.Wait()
	queue1.Close()
	sum := 0
	for _, c := range inCnt {
		sum += c
	}
	if sum != threshold {
		t.Errorf("enqueued entry count not match; expected %d, actual %d", threshold, sum)
	}
	outWg.Wait()

	// count number of dequeued entries
	sum = 0
	for _, c := range counts {
		//if c == 0 {
		//	t.Errorf("goroutine %d processed no entries", i)
		//}
		sum += c
	}
	if sum != threshold {
		t.Errorf("dequeued entry count not match; expected %d, actual %d", threshold, sum)
	}

	// count number of enqueued entries
	if queue2.Len() != threshold {
		t.Errorf("enqueued entry count not match; expected %d, actual %d", threshold, queue2.Len())
	}
	out := make([]int, threshold)
	for i := 0; i < threshold; i++ {
		n, err := queue2.Fetch()
		if err != nil {
			t.Fatalf("dequeue failed")
		}
		out[i] = n
	}
	sort.Ints(out)

	for i := 0; i < threshold; i++ {
		if out[i] != i {
			t.Fatalf("dequeued data incorrect; position %d, value %d", i, out[i])
		}
	}
}
