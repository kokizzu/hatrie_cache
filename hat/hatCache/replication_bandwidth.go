package hatCache

import (
	"io"
	"sync/atomic"
)

type replicationCountingReadCloser struct {
	body  io.ReadCloser
	bytes *atomic.Uint64
}

func (reader *replicationCountingReadCloser) Read(data []byte) (int, error) {
	n, err := reader.body.Read(data)
	if n > 0 {
		reader.bytes.Add(uint64(n))
	}
	return n, err
}

func (reader *replicationCountingReadCloser) Close() error {
	return reader.body.Close()
}
