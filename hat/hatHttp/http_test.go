package hatHttp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGzipHandlerAndLimitedEncodedRequestBody(t *testing.T) {
	handler := GzipHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("response"))
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Accept-Encoding", "gzip")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if got := response.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", got)
	}

	request = httptest.NewRequest(http.MethodPost, "/", io.NopCloser(strings.NewReader(strings.Repeat("x", 9))))
	body, closeBody, ok := LimitedEncodedRequestBody(httptest.NewRecorder(), request, 8)
	if !ok {
		t.Fatal("LimitedEncodedRequestBody() ok = false, want true")
	}
	defer closeBody()
	if _, err := io.ReadAll(body); err != nil {
		t.Fatalf("ReadAll(body) error = %v", err)
	}
	if !TrackedRequestBodyTooLarge(body) {
		t.Fatal("TrackedRequestBodyTooLarge(body) = false, want true")
	}
}
