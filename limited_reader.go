package hatriecache

import (
	"io"

	"hatrie_cache/hat/hatHttp"
)

func limitedReaderExceeded(reader *io.LimitedReader) bool {
	return hatHttp.LimitedReaderExceeded(reader)
}
