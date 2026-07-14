package hatriecache

import (
	"encoding/json"
	"errors"
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
