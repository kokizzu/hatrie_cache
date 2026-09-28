#!/usr/bin/env bash
set -euo pipefail

benchmark_file=$(mktemp hat/hatPgWire/backend_row_description_baseline_temp_XXXXXX_test.go)
trap "rm -f -- '$benchmark_file'" EXIT
cat > "$benchmark_file" <<'EOF'
package hatPgWire

import (
	"net"
	"testing"
	"time"
)

func BenchmarkWriteRowDescriptionAllocatingBaseline(b *testing.B) {
	connection := &backendRowDescriptionBaselineConn{}
	fields := []Field{
		{Name: "id", DataTypeOID: OIDInt8},
		{Name: "name", DataTypeOID: OIDText},
		{Name: "created_at", DataTypeOID: OIDText},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeRowDescription(connection, fields); err != nil {
			b.Fatal(err)
		}
	}
}

type backendRowDescriptionBaselineConn struct{}

func (connection *backendRowDescriptionBaselineConn) Read([]byte) (int, error)       { return 0, nil }
func (connection *backendRowDescriptionBaselineConn) Write(data []byte) (int, error) { return len(data), nil }
func (connection *backendRowDescriptionBaselineConn) Close() error                   { return nil }
func (connection *backendRowDescriptionBaselineConn) LocalAddr() net.Addr            { return backendRowDescriptionBaselineAddr{} }
func (connection *backendRowDescriptionBaselineConn) RemoteAddr() net.Addr           { return backendRowDescriptionBaselineAddr{} }
func (connection *backendRowDescriptionBaselineConn) SetDeadline(time.Time) error    { return nil }
func (connection *backendRowDescriptionBaselineConn) SetReadDeadline(time.Time) error { return nil }
func (connection *backendRowDescriptionBaselineConn) SetWriteDeadline(time.Time) error {
	return nil
}

type backendRowDescriptionBaselineAddr struct{}

func (backendRowDescriptionBaselineAddr) Network() string { return "baseline" }
func (backendRowDescriptionBaselineAddr) String() string  { return "baseline" }
EOF

go test ./hat/hatPgWire/server.go ./hat/hatPgWire/cancel.go ./hat/hatPgWire/metrics.go "$benchmark_file" -run '^$' -bench '^BenchmarkWriteRowDescriptionAllocatingBaseline$' -benchmem -count=1
