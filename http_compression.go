package hatriecache

import (
	"compress/gzip"
	"io"
	"net/http"
	"strconv"
	"strings"

	"hatrie_cache/internal/jsonwire"
)

type gzipResponseWriter struct {
	http.ResponseWriter
	writer     *gzip.Writer
	statusCode int
	wrote      bool
}

func gzipHTTPHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		addVaryHeader(w.Header(), "Accept-Encoding")
		if !requestAcceptsGzip(r) || r.Method == http.MethodHead || r.Header.Get("Range") != "" {
			next.ServeHTTP(w, r)
			return
		}

		gzipWriter := &gzipResponseWriter{ResponseWriter: w}
		defer gzipWriter.Close()
		next.ServeHTTP(gzipWriter, r)
	})
}

func addVaryHeader(header http.Header, value string) {
	for _, existing := range header.Values("Vary") {
		if varyHeaderContains(existing, value) {
			return
		}
	}
	header.Add("Vary", value)
}

func varyHeaderContains(headerValue string, value string) bool {
	for headerValue != "" {
		token, rest, ok := strings.Cut(headerValue, ",")
		token = strings.TrimSpace(token)
		if strings.EqualFold(token, value) || token == "*" {
			return true
		}
		if !ok {
			break
		}
		headerValue = rest
	}
	return false
}

func requestAcceptsGzip(r *http.Request) bool {
	gzipQuality := -1.0
	wildcardQuality := -1.0
	acceptEncoding := r.Header.Get("Accept-Encoding")
	for acceptEncoding != "" {
		part, rest, ok := strings.Cut(acceptEncoding, ",")
		token, quality := parseAcceptEncoding(part)
		switch {
		case strings.EqualFold(token, "gzip"):
			if quality > gzipQuality {
				gzipQuality = quality
			}
		case token == "*":
			if quality > wildcardQuality {
				wildcardQuality = quality
			}
		}
		if !ok {
			break
		}
		acceptEncoding = rest
	}
	if gzipQuality >= 0 {
		return gzipQuality > 0
	}
	return wildcardQuality > 0
}

func parseAcceptEncoding(value string) (string, float64) {
	token, params, _ := strings.Cut(value, ";")
	token = strings.TrimSpace(token)
	if token == "" {
		return "", 0
	}
	quality := 1.0
	for params != "" {
		var part string
		part, params, _ = strings.Cut(params, ";")
		key, raw, hasValue := strings.Cut(strings.TrimSpace(part), "=")
		if !hasValue || !strings.EqualFold(strings.TrimSpace(key), "q") {
			continue
		}
		parsed, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err != nil {
			return token, 0
		}
		if parsed < 0 {
			parsed = 0
		}
		if parsed > 1 {
			parsed = 1
		}
		quality = parsed
	}
	return token, quality
}

func (w *gzipResponseWriter) WriteHeader(statusCode int) {
	if w.wrote {
		return
	}
	w.wrote = true
	w.statusCode = statusCode
	if responseAllowsBody(statusCode) {
		header := w.Header()
		header.Del("Content-Length")
		header.Set("Content-Encoding", "gzip")
		w.writer = jsonwire.AcquireGzipWriter(w.ResponseWriter)
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *gzipResponseWriter) Write(data []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	if !responseAllowsBody(w.statusCode) {
		return 0, nil
	}
	return w.writer.Write(data)
}

func (w *gzipResponseWriter) Close() error {
	if w.writer == nil {
		return nil
	}
	writer := w.writer
	w.writer = nil
	err := writer.Close()
	jsonwire.ReleaseGzipWriter(writer)
	return err
}

func responseAllowsBody(statusCode int) bool {
	return statusCode >= 200 && statusCode != http.StatusNoContent && statusCode != http.StatusNotModified
}

func limitedEncodedRequestBody(w http.ResponseWriter, r *http.Request, limit int64) (io.Reader, func(), bool) {
	encoding := strings.TrimSpace(r.Header.Get("Content-Encoding"))
	if encoding == "" || strings.EqualFold(encoding, "identity") {
		body := http.MaxBytesReader(w, r.Body, limit)
		return body, func() { _ = body.Close() }, true
	}
	if !strings.EqualFold(encoding, "gzip") {
		_ = r.Body.Close()
		http.Error(w, "unsupported request content encoding", http.StatusUnsupportedMediaType)
		return nil, nil, false
	}

	reader, err := gzip.NewReader(r.Body)
	if err != nil {
		_ = r.Body.Close()
		http.Error(w, "invalid gzip request", http.StatusBadRequest)
		return nil, nil, false
	}
	body := http.MaxBytesReader(w, reader, limit)
	return body, func() {
		_ = body.Close()
		_ = r.Body.Close()
	}, true
}
