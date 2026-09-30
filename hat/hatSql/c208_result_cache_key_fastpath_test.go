package hatSql

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"strings"
	"testing"
)

func TestAppendSQLResultCacheBytesPartMatchesString(t *testing.T) {
	value := []byte("encoded-parameters\x00\xff")
	var got strings.Builder
	appendSQLResultCacheBytesPart(&got, value)
	var want strings.Builder
	appendSQLResultCachePart(&want, string(value))
	if got.String() != want.String() {
		t.Fatalf("byte key part = %q, want %q", got.String(), want.String())
	}
}

func TestSQLResultCacheKeyFastpathMatchesStringBaseline(t *testing.T) {
	parameters := []interface{}{int64(42), "ready", true}
	for _, options := range []SQLQueryOptions{{}, {ResultCacheSettingsFingerprint: "tenant-settings-v1"}} {
		got, ok := sqlResultCacheKeyParts("FROM CACHE('orders') SELECT id WHERE status = ?", parameters, options)
		want, wantOK := sqlResultCacheKeyPartsStringBaseline("FROM CACHE('orders') SELECT id WHERE status = ?", parameters, options)
		if !ok || !wantOK || got != want {
			t.Fatalf("key mismatch: got %q/%t, want %q/%t", got, ok, want, wantOK)
		}
	}
}

func BenchmarkSQLResultCacheKeyFastpathPaired(b *testing.B) {
	parameters := []interface{}{int64(42), "ready", true}
	source := "FROM CACHE('orders') SELECT id WHERE status = ?"
	b.Run("string-baseline", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, ok := sqlResultCacheKeyPartsStringBaseline(source, parameters, SQLQueryOptions{}); !ok {
				b.Fatal("baseline key rejected benchmark input")
			}
		}
	})
	b.Run("byte-fastpath", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, ok := sqlResultCacheKeyParts(source, parameters, SQLQueryOptions{}); !ok {
				b.Fatal("fastpath key rejected benchmark input")
			}
		}
	})
}

func sqlResultCacheKeyPartsStringBaseline(source string, parameters []interface{}, options SQLQueryOptions) (string, bool) {
	if len(options.ResultCacheSettingsFingerprint) > MaxSQLResultCacheSettingsFingerprintBytes {
		return "", false
	}
	var encoded bytes.Buffer
	if err := gob.NewEncoder(&encoded).Encode(parameters); err != nil {
		return "", false
	}
	var key strings.Builder
	key.WriteString("hatsql-result-cache-v2")
	appendSQLResultCachePart(&key, source)
	appendSQLResultCachePart(&key, encoded.String())
	appendSQLResultCachePart(&key, string(options.Collation))
	appendSQLResultCachePart(&key, options.PreparedSchemaVersion)
	appendSQLResultCachePart(&key, strconv.FormatBool(options.PlanSnapshot != nil))
	if options.ResultCacheSettingsFingerprint != "" {
		appendSQLResultCachePart(&key, options.ResultCacheSettingsFingerprint)
	}
	return key.String(), true
}
