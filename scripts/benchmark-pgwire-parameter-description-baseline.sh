#!/usr/bin/env bash
set -euo pipefail

benchmark_file=$(mktemp hat/hatPgWire/backend_parameter_description_baseline_temp_XXXXXX_test.go)
trap "rm -f -- '$benchmark_file'" EXIT
cat > "$benchmark_file" <<'EOF'
package hatPgWire

import (
	"net"
	"testing"
	"time"
)

func BenchmarkWriteParameterDescriptionAllocatingBaseline(b *testing.B) {
	connection := &backendParameterBaselineConn{}
	parameterTypes := []uint32{OIDInt8, OIDText, OIDFloat8, OIDInt4, OIDBool, OIDText}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeParameterDescription(connection, parameterTypes); err != nil {
			b.Fatal(err)
		}
	}
}

type backendParameterBaselineConn struct{}

func (connection *backendParameterBaselineConn) Read([]byte) (int, error)       { return 0, nil }
func (connection *backendParameterBaselineConn) Write(data []byte) (int, error) { return len(data), nil }
func (connection *backendParameterBaselineConn) Close() error                   { return nil }
func (connection *backendParameterBaselineConn) LocalAddr() net.Addr            { return backendParameterBaselineAddr{} }
func (connection *backendParameterBaselineConn) RemoteAddr() net.Addr           { return backendParameterBaselineAddr{} }
func (connection *backendParameterBaselineConn) SetDeadline(time.Time) error    { return nil }
func (connection *backendParameterBaselineConn) SetReadDeadline(time.Time) error { return nil }
func (connection *backendParameterBaselineConn) SetWriteDeadline(time.Time) error {
	return nil
}

type backendParameterBaselineAddr struct{}

func (backendParameterBaselineAddr) Network() string { return "baseline" }
func (backendParameterBaselineAddr) String() string  { return "baseline" }
EOF

go test ./hat/hatPgWire/server.go ./hat/hatPgWire/cancel.go ./hat/hatPgWire/metrics.go "$benchmark_file" -run '^$' -bench '^BenchmarkWriteParameterDescriptionAllocatingBaseline$' -benchmem -count=1
