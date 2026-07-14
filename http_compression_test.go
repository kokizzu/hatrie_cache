package hatriecache

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type closeTrackingBody struct {
	*strings.Reader
	closed bool
}

func newCloseTrackingBody(data []byte) *closeTrackingBody {
	return &closeTrackingBody{Reader: strings.NewReader(string(data))}
}

func (body *closeTrackingBody) Close() error {
	body.closed = true
	return nil
}

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

func TestGzipHTTPHandlerAddsVaryWhenResponseIsPlain(t *testing.T) {
	handler := gzipHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("plain"))
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Accept-Encoding", "br")

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, request)
	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.Code)
	}
	if got := resp.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("Content-Encoding = %q, want empty", got)
	}
	if got := resp.Header().Values("Vary"); !headerValuesContain(got, "Accept-Encoding") {
		t.Fatalf("Vary = %#v, want Accept-Encoding", got)
	}
	if got := resp.Body.String(); got != "plain" {
		t.Fatalf("body = %q, want plain", got)
	}
}

func TestRequestAcceptsGzipRespectsQuality(t *testing.T) {
	tests := []struct {
		name           string
		acceptEncoding string
		want           bool
	}{
		{
			name:           "explicit gzip",
			acceptEncoding: "br, gzip",
			want:           true,
		},
		{
			name:           "explicit gzip zero overrides wildcard",
			acceptEncoding: "gzip;q=0, *;q=1",
			want:           false,
		},
		{
			name:           "wildcard allows gzip",
			acceptEncoding: "br;q=1, *;q=0.5",
			want:           true,
		},
		{
			name:           "repeated gzip keeps highest quality",
			acceptEncoding: "gzip;q=0, br;q=1, gzip;q=0.8",
			want:           true,
		},
		{
			name:           "invalid q disables token",
			acceptEncoding: "gzip;q=bogus",
			want:           false,
		},
		{
			name:           "valueless params before q are ignored",
			acceptEncoding: "gzip; foo; q=0.7",
			want:           true,
		},
		{
			name:           "unsupported encoding only",
			acceptEncoding: "br",
			want:           false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.Header.Set("Accept-Encoding", tt.acceptEncoding)
			if got := requestAcceptsGzip(request); got != tt.want {
				t.Fatalf("requestAcceptsGzip(%q) = %v, want %v", tt.acceptEncoding, got, tt.want)
			}
		})
	}
}

func TestAddVaryHeaderDeduplicatesCommaSeparatedValues(t *testing.T) {
	header := http.Header{}
	header.Add("Vary", "Accept, Accept-Encoding")
	addVaryHeader(header, "accept-encoding")
	addVaryHeader(header, "Accept")
	addVaryHeader(header, "Origin")

	if got := header.Values("Vary"); len(got) != 2 {
		t.Fatalf("Vary values = %#v, want original plus Origin", got)
	}
	if got := header.Values("Vary"); !headerValuesContain(got, "Accept-Encoding") || !headerValuesContain(got, "Accept") || !headerValuesContain(got, "Origin") {
		t.Fatalf("Vary = %#v, want Accept, Accept-Encoding, Origin", got)
	}
}

func TestAddVaryHeaderRespectsWildcard(t *testing.T) {
	header := http.Header{}
	header.Add("Vary", "Accept, *")
	addVaryHeader(header, "Accept-Encoding")

	if got := header.Values("Vary"); len(got) != 1 || got[0] != "Accept, *" {
		t.Fatalf("Vary values = %#v, want original wildcard only", got)
	}
}

func TestVaryHeaderContainsTrimsTokens(t *testing.T) {
	if !varyHeaderContains(" Accept ,  Accept-Encoding ", "accept-encoding") {
		t.Fatal("varyHeaderContains() = false, want true for trimmed case-insensitive token")
	}
	if varyHeaderContains("Accept-Language", "Accept") {
		t.Fatal("varyHeaderContains() = true for partial token, want false")
	}
}

func TestLimitedEncodedRequestBodyClosesIdentityBody(t *testing.T) {
	body := newCloseTrackingBody([]byte(`{"ok":true}`))
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Body = body

	reader, closeBody, ok := limitedEncodedRequestBody(httptest.NewRecorder(), request, 1024)
	if !ok {
		t.Fatal("limitedEncodedRequestBody(identity) ok = false, want true")
	}
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatalf("ReadAll(identity body) error = %v", err)
	}
	closeBody()
	if !body.closed {
		t.Fatal("identity request body was not closed")
	}
}

func TestLimitedEncodedRequestBodyClosesGzipBody(t *testing.T) {
	body := newCloseTrackingBody(gzipHTTPTestBytes(t, []byte(`{"ok":true}`)))
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Body = body
	request.Header.Set("Content-Encoding", "gzip")

	reader, closeBody, ok := limitedEncodedRequestBody(httptest.NewRecorder(), request, 1024)
	if !ok {
		t.Fatal("limitedEncodedRequestBody(gzip) ok = false, want true")
	}
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatalf("ReadAll(gzip body) error = %v", err)
	}
	closeBody()
	if !body.closed {
		t.Fatal("gzip request body was not closed")
	}
}

func TestLimitedEncodedRequestBodyTracksDecodedLimit(t *testing.T) {
	body := newCloseTrackingBody(gzipHTTPTestBytes(t, []byte(strings.Repeat("x", 32))))
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Body = body
	request.Header.Set("Content-Encoding", "gzip")

	reader, closeBody, ok := limitedEncodedRequestBody(httptest.NewRecorder(), request, 8)
	if !ok {
		t.Fatal("limitedEncodedRequestBody(gzip) ok = false, want true")
	}
	defer closeBody()
	if _, err := io.ReadAll(reader); err == nil {
		t.Fatal("ReadAll(oversized gzip body) error = nil, want max bytes error")
	}
	if !trackedRequestBodyTooLarge(reader) {
		t.Fatal("trackedRequestBodyTooLarge() = false, want true")
	}
}

func TestLimitedEncodedRequestBodyClosesInvalidGzipBody(t *testing.T) {
	body := newCloseTrackingBody([]byte("not gzip"))
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Body = body
	request.Header.Set("Content-Encoding", "gzip")

	_, _, ok := limitedEncodedRequestBody(httptest.NewRecorder(), request, 1024)
	if ok {
		t.Fatal("limitedEncodedRequestBody(invalid gzip) ok = true, want false")
	}
	if !body.closed {
		t.Fatal("invalid gzip request body was not closed")
	}
}

func gzipHTTPTestBytes(t *testing.T, data []byte) []byte {
	t.Helper()

	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(data); err != nil {
		t.Fatalf("gzip Write() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("gzip Close() error = %v", err)
	}
	return buffer.Bytes()
}
