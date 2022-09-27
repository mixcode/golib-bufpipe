package pipe

import (
	"context"
	"io"
)

// A Pipe for []Byte with io.ReadWriteCloser and io.ReadFrom interface.
type BytePipe struct {
	*Pipe[[]byte]

	activeBuf []byte
}

// Create a new BytePipe.
func NewBytePipe() *BytePipe {
	return &BytePipe{Pipe: NewPipe[[]byte]()}
}

// io.Reader inteface for BytePipe.
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
			bp.activeBuf, err = bp.Pipe.Read(context.Background())
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
// Closing it prevents data from writing, but Read()/Fetch()/Receive() is valid until io.EOF reached.
// Check for returning io.EOF, or IsEOF() to know the end of the stream.
func (bp *BytePipe) Close() bool {
	return bp.Pipe.Close()
}

// Check if the BytePipe is closed and no data left for read.
func (bp *BytePipe) IsEOF() bool {
	return bp.readClosed && len(bp.activeBuf) == 0
}

var (
	ReadFromBufSize = 1 * 1024 * 1024
)

// io.ReaderFrom interface for BytePipe.
func (bp *BytePipe) ReadFrom(r io.Reader) (n int64, err error) {
	bufSize := ReadFromBufSize
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
// The provided data is internally copied when writing.
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
