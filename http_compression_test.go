package hatriecache

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGzipHTTPHandlerLeavesNoBodyResponsesUncompressed(t *testing.T) {
	handler := gzipHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Accept-Encoding", "gzip")

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, request)
	if resp.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.Code)
	}
	if got := resp.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("Content-Encoding = %q, want empty", got)
	}
	if resp.Body.Len() != 0 {
		t.Fatalf("body length = %d, want empty", resp.Body.Len())
	}
}

func TestGzipHTTPHandlerSkipsRangeRequests(t *testing.T) {
	handler := gzipHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("partial"))
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Accept-Encoding", "gzip")
	request.Header.Set("Range", "bytes=0-6")

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, request)
	if resp.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", resp.Code)
	}
	if got := resp.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("Content-Encoding = %q, want empty", got)
	}
	if got := resp.Body.String(); got != "partial" {
		t.Fatalf("body = %q, want partial", got)
	}
}
