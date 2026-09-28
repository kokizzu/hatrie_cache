#!/usr/bin/env bash
set -euo pipefail

benchmark_file=$(mktemp hat/hatPgWire/backend_scalar_message_baseline_temp_XXXXXX_test.go)
trap "rm -f -- '$benchmark_file'" EXIT
cat > "$benchmark_file" <<'EOF'
package hatPgWire

import (
	"net"
	"testing"
	"time"
)

func BenchmarkWriteParameterStatusAllocatingBaseline(b *testing.B) {
	connection := &backendScalarBaselineConn{}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeParameterStatus(connection, "server_version", "16.0"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteCommandTagAllocatingBaseline(b *testing.B) {
	connection := &backendScalarBaselineConn{}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeMessage(connection, 'C', appendCString(nil, "SELECT 1")); err != nil {
			b.Fatal(err)
		}
	}
}

type backendScalarBaselineConn struct{}

func (connection *backendScalarBaselineConn) Read([]byte) (int, error)       { return 0, nil }
func (connection *backendScalarBaselineConn) Write(data []byte) (int, error) { return len(data), nil }
func (connection *backendScalarBaselineConn) Close() error                   { return nil }
func (connection *backendScalarBaselineConn) LocalAddr() net.Addr            { return backendScalarBaselineAddr{} }
func (connection *backendScalarBaselineConn) RemoteAddr() net.Addr           { return backendScalarBaselineAddr{} }
func (connection *backendScalarBaselineConn) SetDeadline(time.Time) error    { return nil }
func (connection *backendScalarBaselineConn) SetReadDeadline(time.Time) error { return nil }
func (connection *backendScalarBaselineConn) SetWriteDeadline(time.Time) error {
	return nil
}

type backendScalarBaselineAddr struct{}

func (backendScalarBaselineAddr) Network() string { return "baseline" }
func (backendScalarBaselineAddr) String() string  { return "baseline" }
EOF

go test ./hat/hatPgWire/server.go ./hat/hatPgWire/cancel.go ./hat/hatPgWire/metrics.go "$benchmark_file" -run '^$' -bench '^BenchmarkWrite(ParameterStatus|CommandTag)AllocatingBaseline$' -benchmem -count=1
