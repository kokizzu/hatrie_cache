package hatriecache

import (
	"hatrie_cache/hat/hatCodec"
)

func jsonEncodedSize(value interface{}) (int64, error) {
	return hatCodec.JSONEncodedSize(value)
}

func jsonEncodedSizeWithin(value interface{}, limit int64) (int64, bool, error) {
	return hatCodec.JSONEncodedSizeWithin(value, limit)
}

func jsonEncodedString(value interface{}) (string, error) {
	return hatCodec.JSONEncodedString(value)
}
