package hatHttp_test

import (
	"io"
	"strings"
	"testing"

	"hatrie_cache/hat/hatHttp"
)

func TestLimitedReaderExceeded(t *testing.T) {
	within := &io.LimitedReader{R: strings.NewReader("ok"), N: 3}
	if hatHttp.LimitedReaderExceeded(within) {
		t.Fatal("LimitedReaderExceeded() reported an in-limit reader")
	}
	over := &io.LimitedReader{R: strings.NewReader("toolong"), N: 3}
	if !hatHttp.LimitedReaderExceeded(over) {
		t.Fatal("LimitedReaderExceeded() did not report exhausted reader")
	}
}
