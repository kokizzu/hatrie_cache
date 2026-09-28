#!/usr/bin/env bash
set -euo pipefail

benchmark_file=$(mktemp hat/hatPgWire/backend_fixed_message_baseline_temp_XXXXXX_test.go)
trap "rm -f -- '$benchmark_file'" EXIT
cat > "$benchmark_file" <<'EOF'
package hatPgWire

import (
	"net"
	"testing"
	"time"
)

func BenchmarkWriteAuthenticationOKAllocatingBaseline(b *testing.B) {
	connection := &backendFixedBaselineConn{}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeAuthenticationOK(connection); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteReadyForQueryAllocatingBaseline(b *testing.B) {
	connection := &backendFixedBaselineConn{}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeReadyForQuery(connection); err != nil {
			b.Fatal(err)
		}
	}
}

type backendFixedBaselineConn struct{}

func (connection *backendFixedBaselineConn) Read([]byte) (int, error)       { return 0, nil }
func (connection *backendFixedBaselineConn) Write(data []byte) (int, error) { return len(data), nil }
func (connection *backendFixedBaselineConn) Close() error                   { return nil }
func (connection *backendFixedBaselineConn) LocalAddr() net.Addr            { return backendFixedBaselineAddr{} }
func (connection *backendFixedBaselineConn) RemoteAddr() net.Addr           { return backendFixedBaselineAddr{} }
func (connection *backendFixedBaselineConn) SetDeadline(time.Time) error    { return nil }
func (connection *backendFixedBaselineConn) SetReadDeadline(time.Time) error { return nil }
func (connection *backendFixedBaselineConn) SetWriteDeadline(time.Time) error {
	return nil
}

type backendFixedBaselineAddr struct{}

func (backendFixedBaselineAddr) Network() string { return "baseline" }
func (backendFixedBaselineAddr) String() string  { return "baseline" }
EOF

go test ./hat/hatPgWire/server.go ./hat/hatPgWire/cancel.go ./hat/hatPgWire/metrics.go "$benchmark_file" -run '^$' -bench '^BenchmarkWrite(AuthenticationOK|ReadyForQuery)AllocatingBaseline$' -benchmem -count=1
