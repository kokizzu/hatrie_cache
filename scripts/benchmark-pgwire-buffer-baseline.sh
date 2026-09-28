#!/usr/bin/env bash
set -euo pipefail

stub_file="hat/hatPgWire/frontend_buffer_baseline_stub_temp.go"
benchmark_file="hat/hatPgWire/frontend_buffer_baseline_temp_test.go"
trap 'rm -f "$stub_file" "$benchmark_file"' EXIT
cat > "$stub_file" <<'EOF'
package hatPgWire

import "net"

func readFrontendMessageInto(connection net.Conn, maxMessage int, buffer []byte) (byte, []byte, []byte, error) {
	messageType, body, err := readFrontendMessage(connection, maxMessage)
	return messageType, body, buffer, err
}
EOF
cat > "$benchmark_file" <<'EOF'
package hatPgWire

import (
	"bytes"
	"testing"
)

func pgWireBaselineMessages() []byte {
	data := make([]byte, 0, 128*261)
	for index := 0; index < 128; index++ {
		data = appendFrontendTestMessage(data, 'Q', bytes.Repeat([]byte{byte(index)}, 256))
	}
	return data
}

var pgWireBaselineSink int

func BenchmarkReadFrontendMessageAllocatingBaseline(b *testing.B) {
	data := pgWireBaselineMessages()
	connection := &frontendMessageTestConn{reader: bytes.NewReader(data)}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		connection.reader.Reset(data)
		bytesRead := 0
		for message := 0; message < 128; message++ {
			_, body, err := readFrontendMessage(connection, 1<<20)
			if err != nil {
				b.Fatal(err)
			}
			bytesRead += len(body)
		}
		pgWireBaselineSink = bytesRead
	}
}
EOF
go test ./hat/hatPgWire/server.go ./hat/hatPgWire/cancel.go ./hat/hatPgWire/metrics.go ./hat/hatPgWire/frontend_buffer_reuse_test.go "$stub_file" "$benchmark_file" -run '^$' -bench '^BenchmarkReadFrontendMessageAllocatingBaseline$' -benchmem -count=1
