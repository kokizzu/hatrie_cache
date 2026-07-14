package hatriecache

import (
	"errors"
	"strings"

	json "github.com/goccy/go-json"
)

const maxInt64 = int64(^uint64(0) >> 1)

var errJSONEncodedSizeTooLarge = errors.New("hatriecache: JSON encoded size is too large")

type jsonSizeWriter struct {
	size int64
}

func (writer *jsonSizeWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > maxInt64-writer.size {
		return 0, errJSONEncodedSizeTooLarge
	}
	writer.size += int64(len(data))
	return len(data), nil
}

func jsonEncodedSize(value interface{}) (int64, error) {
	var writer jsonSizeWriter
	if err := json.NewEncoder(&writer).Encode(value); err != nil {
		return 0, err
	}
	if writer.size == 0 {
		return 0, nil
	}
	return writer.size - 1, nil
}

func jsonEncodedString(value interface{}) (string, error) {
	var builder strings.Builder
	if err := json.NewEncoder(&builder).Encode(value); err != nil {
		return "", err
	}
	return strings.TrimSuffix(builder.String(), "\n"), nil
}
