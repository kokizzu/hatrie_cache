package hatSql_test

import (
	"bytes"
	"context"
	"os"
	"testing"

	"hatrie_cache/hat/hatCodec"
	"hatrie_cache/hat/hatSql"
)

func TestEncryptedSQLSortSpillHidesPlaintextAndExecutes(t *testing.T) {
	cipher, err := hatCodec.NewStreamCipher("test-key", bytes.Repeat([]byte{3}, 32))
	if err != nil {
		t.Fatal(err)
	}
	inspected := false
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM VALUES ('secret-value'), ('alpha') AS values(name) SELECT name ORDER BY name", hatSql.SQLSourceResolverFunc(nil), hatSql.SQLQueryOptions{
		MaxSortBytes:   1,
		MaxSpillBytes:  1 << 20,
		SpillDirectory: t.TempDir(),
		SpillCipher:    cipher,
		SpillFaults: &hatSql.SQLSpillFaults{BeforeRead: func(kind, path string) error {
			if kind != "sort" {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if bytes.Contains(data, []byte("secret-value")) {
				t.Fatal("plaintext was present in encrypted SQL spill file")
			}
			inspected = true
			return nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !inspected {
		t.Fatal("encrypted SQL spill file was not inspected")
	}
	if len(result.Rows) != 2 || result.Rows[0]["name"] != "alpha" || result.Rows[1]["name"] != "secret-value" {
		t.Fatalf("encrypted spill result = %#v", result.Rows)
	}
}
