#!/usr/bin/env bash
set -euo pipefail

benchmark_file=$(mktemp hat/hatPgWire/backend_response_baseline_temp_XXXXXX_test.go)
trap "rm -f -- '$benchmark_file'" EXIT
cat > "$benchmark_file" <<'EOF'
package hatPgWire

import (
	"net"
	"testing"
	"time"
)

func BenchmarkWriteMessageAllocatingBaseline(b *testing.B) {
	connection := &backendMessageBaselineConn{}
	body := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeMessage(connection, 'D', body); err != nil {
			b.Fatal(err)
		}
	}
}

type backendMessageBaselineConn struct{}

func (connection *backendMessageBaselineConn) Read([]byte) (int, error)       { return 0, nil }
func (connection *backendMessageBaselineConn) Write(data []byte) (int, error) { return len(data), nil }
func (connection *backendMessageBaselineConn) Close() error                   { return nil }
func (connection *backendMessageBaselineConn) LocalAddr() net.Addr            { return backendMessageBaselineAddr{} }
func (connection *backendMessageBaselineConn) RemoteAddr() net.Addr           { return backendMessageBaselineAddr{} }
func (connection *backendMessageBaselineConn) SetDeadline(time.Time) error    { return nil }
func (connection *backendMessageBaselineConn) SetReadDeadline(time.Time) error { return nil }
func (connection *backendMessageBaselineConn) SetWriteDeadline(time.Time) error {
	return nil
}

type backendMessageBaselineAddr struct{}

func (backendMessageBaselineAddr) Network() string { return "baseline" }
func (backendMessageBaselineAddr) String() string  { return "baseline" }
EOF

go test ./hat/hatPgWire/server.go ./hat/hatPgWire/cancel.go ./hat/hatPgWire/metrics.go "$benchmark_file" -run '^$' -bench '^BenchmarkWriteMessageAllocatingBaseline$' -benchmem -count=1
