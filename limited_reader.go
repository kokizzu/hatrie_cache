package hatriecache

import "io"

func limitedReaderExceeded(reader *io.LimitedReader) bool {
	if reader.N <= 0 {
		return true
	}
	_, _ = io.Copy(io.Discard, reader)
	return reader.N <= 0
}
