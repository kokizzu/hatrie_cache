package hatCodec_test

import (
	"testing"

	json "github.com/goccy/go-json"
	"hatrie_cache/hat/hatCodec"
)

func TestJSONEncodedSizePublicContract(t *testing.T) {
	value := map[string]interface{}{"name": "cache", "count": 3}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	size, err := hatCodec.JSONEncodedSize(value)
	if err != nil || size != int64(len(encoded)) {
		t.Fatalf("JSONEncodedSize() = %d/%v, want %d/nil", size, err, len(encoded))
	}
	if _, within, err := hatCodec.JSONEncodedSizeWithin(value, size-1); err != nil || within {
		t.Fatalf("JSONEncodedSizeWithin() = %v/%v, want false/nil", within, err)
	}
}
