package bufpipe

import (
	"context"
	"io"
)

// A Pipe of []Byte with io.Reader, io.WriteCloser and io.ReadFrom interface.
type BytePipe struct {
	Pipe[[]byte] // Pipe of []byte data block

	ReadFromSize int // size of []byte data blocks created by ReadFrom()
	activeBuf    []byte
}

var (
	ReadFromBufSize = 1 * 1024 * 1024 // default size for BytePipe.ReadFromSize
)

// Create a new BytePipe.
func NewBytePipe() *BytePipe {
	return &BytePipe{Pipe: *NewPipe[[]byte](), ReadFromSize: ReadFromBufSize}
}

// io.Reader inteface for BytePipe.
// The data is internally copied from the Pipe to the provided buffer.
// Use Fetch() or Receive() for zero-copy data receiving.
func (bp *BytePipe) Read(p []byte) (n int, err error) {

	if bp.readClosed && len(bp.activeBuf) == 0 {
		err = io.EOF
		return
	}

	for len(p) > 0 {
		if len(bp.activeBuf) == 0 {
			bp.activeBuf, err = bp.Pipe.Fetch()
			//log.Printf("activeBuf: %d, %v", len(bp.activeBuf), err)
			if err != ErrNoData && err != io.EOF {
				return
			}
			if n > 0 {
				// receive buffer has some data
				err = nil
				return
			}
			if err == io.EOF {
				return
			}
			bp.activeBuf, err = bp.Pipe.Receive(context.Background())
			if err != nil {
				return
			}
		}

		sz := copy(p, bp.activeBuf)
		p = p[sz:]
		n += sz
		bp.activeBuf = bp.activeBuf[sz:]
	}

	return
}

// io.Closer for io.WriteCloser, but not for io.ReadCloser.
// Closing BytePipe prevents data from writing, but Read()/Fetch()/Receive() are OK until io.EOF reached.
// Check for returning io.EOF, or EOF() to know the end of the stream.
func (bp *BytePipe) Close() bool {
	return bp.Pipe.Close()
}

// Check if the BytePipe is closed and no data left for read.
func (bp *BytePipe) EOF() bool {
	return bp.readClosed && len(bp.activeBuf) == 0
}

// io.ReaderFrom interface for BytePipe.
// Incoming data are partitioned into multiple []byte of bp.ReadFromSize and stored.
func (bp *BytePipe) ReadFrom(r io.Reader) (n int64, err error) {
	bufSize := bp.ReadFromSize
	if bufSize == 0 {
		bufSize = ReadFromBufSize
	}
	for {
		buf := make([]byte, bufSize)
		sz, e := io.ReadFull(r, buf)
		if sz > 0 {
			bp.Append(buf[:sz])
		}
		n += int64(sz)
		if e != nil {
			if e == io.ErrUnexpectedEOF || e == io.EOF {
				// end of data
				e = nil
			}
			err = e
			break
		}
	}
	return
}

// io.Writer interface for BytePipe.
// The data is copied from the provided buffer to an internal buffer when writing.
// Use Append() for zero-copy data passing.
func (bp *BytePipe) Write(p []byte) (n int, err error) {
	l := len(p)
	if l == 0 {
		return 0, nil
	}
	data := make([]byte, l)
	copy(data, p)
	_, err = bp.Append(data)
	if err == nil {
		n = l
	}
	return
}
