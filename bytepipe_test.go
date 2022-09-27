package pipe

import (
	"io"
	"sync"
	"testing"
)

func TestBytePipeWrite(t *testing.T) {
	bp := NewBytePipe()

	var wg sync.WaitGroup

	receiveBuf := make([]byte, 0)
	wg.Add(1)
	go func() {
		// reader thread
		defer wg.Done()
		readBuf := make([]byte, 256)
		for {
			n, err := bp.Read(readBuf)
			if err != nil {
				if err == io.EOF {
					return
				}
				t.Error(err)
			}
			receiveBuf = append(receiveBuf, readBuf[:n]...)
		}
	}()
	// write data
	sendBuf := make([]byte, 256)
	written := 0
	for i := 2; i < 256; i++ {
		for j := 0; j < i; j++ {
			sendBuf[j] = byte(i)
		}
		n, err := bp.Write(sendBuf[:i])
		if err != nil {
			t.Error(err)
		}
		if n != i {
			t.Errorf("write data size mismatch; expected %d, actual %d", i, n)
		}
		written += n
	}
	bp.Close()
	wg.Wait()

	// verify the data
	if len(receiveBuf) != written {
		t.Fatalf("data size mismatch: written %d, read %d", written, len(receiveBuf))
	}
	offset := 0
	for i := 2; i < 256; i++ {
		for j := 0; j < i; j++ {
			k := offset + j
			if int(receiveBuf[k]) != i {
				t.Fatalf("read data mismatch: offset %d, value %d", k, receiveBuf[k])
			}
		}
		offset += i
	}

}

func TestBytePipeAppend(t *testing.T) {
	bp := NewBytePipe()

	var wg sync.WaitGroup

	receiveBuf := make([]byte, 0)
	wg.Add(1)
	go func() {
		// reader thread
		defer wg.Done()
		readBuf := make([]byte, 256)
		for {
			n, err := bp.Read(readBuf)
			if err != nil {
				if err == io.EOF {
					return
				}
				t.Error(err)
			}
			receiveBuf = append(receiveBuf, readBuf[:n]...)
		}
	}()
	// write data
	written := 0
	for i := 2; i < 256; i++ {
		sendBuf := make([]byte, i)
		for j := 0; j < i; j++ {
			sendBuf[j] = byte(i)
		}
		_, err := bp.Append(sendBuf)
		if err != nil {
			t.Error(err)
		}
		written += len(sendBuf)
	}
	bp.Close()
	wg.Wait()

	// verify the data
	if len(receiveBuf) != written {
		t.Fatalf("data size mismatch: written %d, read %d", written, len(receiveBuf))
	}
	offset := 0
	for i := 2; i < 256; i++ {
		for j := 0; j < i; j++ {
			k := offset + j
			if int(receiveBuf[k]) != i {
				t.Fatalf("read data mismatch: offset %d, value %d", k, receiveBuf[k])
			}
		}
		offset += i
	}

}

func TestBytePipeReadFrom(t *testing.T) {
	bp := NewBytePipe()
	bpr := NewBytePipe()

	var wg sync.WaitGroup

	wg.Add(1)
	var readFromSz int64
	go func() {
		// reader thread
		defer wg.Done()
		defer bpr.Close()
		var e error
		readFromSz, e = bpr.ReadFrom(bp)
		if e != nil {
			t.Error(e)
		}
	}()
	// write data
	written := 0
	for i := 2; i < 256; i++ {
		sendBuf := make([]byte, i)
		for j := 0; j < i; j++ {
			sendBuf[j] = byte(i)
		}
		_, err := bp.Append(sendBuf)
		if err != nil {
			t.Error(err)
		}
		written += len(sendBuf)
	}
	bp.Close()
	wg.Wait()
	if int64(written) != readFromSz {
		t.Fatalf("data size mismatch: written %d, read %d", written, readFromSz)
	}

	receiveBuf, err := io.ReadAll(bpr)
	if err != nil {
		t.Fatal(err)
	}

	// verify the data
	if len(receiveBuf) != written {
		t.Fatalf("data size mismatch: written %d, read %d", written, len(receiveBuf))
	}
	offset := 0
	for i := 2; i < 256; i++ {
		for j := 0; j < i; j++ {
			k := offset + j
			if int(receiveBuf[k]) != i {
				t.Fatalf("read data mismatch: offset %d, value %d", k, receiveBuf[k])
			}
		}
		offset += i
	}

}
