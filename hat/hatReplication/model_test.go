package hatReplication

import "testing"

func TestParseOutboxCodec(t *testing.T) {
	for _, value := range []string{"", "binary", "JSON"} {
		if _, err := ParseOutboxCodec(value); err != nil {
			t.Fatalf("ParseOutboxCodec(%q) error = %v", value, err)
		}
	}
	if _, err := ParseOutboxCodec("gob"); err == nil {
		t.Fatal("ParseOutboxCodec(gob) error = nil, want error")
	}
}
