#!/usr/bin/env bash
set -euo pipefail

baseline_file="hat/hatHash/hash_key_fastpath_baseline_temp_test.go"
trap 'rm -f "$baseline_file"' EXIT
cat > "$baseline_file" <<'EOF'
package hatHash

import (
	"encoding/binary"
	"testing"
)

var hashKeyFastpathBaselineSink uint64

func BenchmarkFNV1a64Uint64EncodedBaseline(b *testing.B) {
	var encoded [8]byte
	value := uint64(0x1020304050607080)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		binary.BigEndian.PutUint64(encoded[:], value+uint64(index))
		hashKeyFastpathBaselineSink = FNV1a64(encoded[:])
	}
}
EOF
go test ./hat/hatHash/hash.go "$baseline_file" -run '^$' -bench '^BenchmarkFNV1a64Uint64EncodedBaseline$' -benchmem -count=1
