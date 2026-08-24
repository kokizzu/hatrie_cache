// Package hatCodec provides allocation-conscious wire-format helpers without
// cache, trie, or transport dependencies.
package hatCodec

import (
	"errors"
	"strings"

	json "github.com/goccy/go-json"
)

const maxInt64 = int64(^uint64(0) >> 1)

var (
	errJSONEncodedSizeTooLarge      = errors.New("hatriecache: JSON encoded size is too large")
	errJSONEncodedSizeLimitExceeded = errors.New("hatriecache: JSON encoded size limit exceeded")
)

type jsonSizeWriter struct {
	size, limit int64
	limited     bool
}

func (writer *jsonSizeWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > maxInt64-writer.size {
		return 0, errJSONEncodedSizeTooLarge
	}
	next := writer.size + int64(len(data))
	if writer.limited && next > writer.limit {
		return 0, errJSONEncodedSizeLimitExceeded
	}
	writer.size = next
	return len(data), nil
}

// JSONEncodedSize returns the bytes required for the canonical JSON encoding.
func JSONEncodedSize(value interface{}) (int64, error) {
	var writer jsonSizeWriter
	if err := json.NewEncoder(&writer).Encode(value); err != nil {
		return 0, err
	}
	if writer.size == 0 {
		return 0, nil
	}
	return writer.size - 1, nil
}

// JSONEncodedSizeWithin returns the canonical JSON size only when it does not
// exceed limit. It stops encoding as soon as the limit is exceeded.
func JSONEncodedSizeWithin(value interface{}, limit int64) (int64, bool, error) {
	if limit < 0 {
		return 0, false, nil
	}
	if limit == maxInt64 {
		size, err := JSONEncodedSize(value)
		return size, err == nil, err
	}
	writer := jsonSizeWriter{limit: limit + 1, limited: true}
	if err := json.NewEncoder(&writer).Encode(value); err != nil {
		if errors.Is(err, errJSONEncodedSizeLimitExceeded) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if writer.size == 0 {
		return 0, true, nil
	}
	return writer.size - 1, true, nil
}

// JSONEncodedString returns canonical JSON without Encoder's trailing newline.
func JSONEncodedString(value interface{}) (string, error) {
	var builder strings.Builder
	if err := json.NewEncoder(&builder).Encode(value); err != nil {
		return "", err
	}
	return strings.TrimSuffix(builder.String(), "\n"), nil
}
