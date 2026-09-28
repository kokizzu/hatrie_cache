#!/usr/bin/env bash
set -euo pipefail

benchmark_file=$(mktemp hat/hatPgWire/backend_row_encoding_baseline_temp_XXXXXX_test.go)
trap "rm -f -- '$benchmark_file'" EXIT
cat > "$benchmark_file" <<'EOF'
package hatPgWire

import (
	"net"
	"testing"
	"time"
)

func BenchmarkWriteDataRowAllocatingBaseline(b *testing.B) {
	connection := &backendRowBaselineConn{}
	first := "alpha"
	second := "beta"
	row := []*string{&first, nil, &second}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeDataRow(connection, row); err != nil {
			b.Fatal(err)
		}
	}
}

type backendRowBaselineConn struct{}

func (connection *backendRowBaselineConn) Read([]byte) (int, error)       { return 0, nil }
func (connection *backendRowBaselineConn) Write(data []byte) (int, error) { return len(data), nil }
func (connection *backendRowBaselineConn) Close() error                   { return nil }
func (connection *backendRowBaselineConn) LocalAddr() net.Addr            { return backendRowBaselineAddr{} }
func (connection *backendRowBaselineConn) RemoteAddr() net.Addr           { return backendRowBaselineAddr{} }
func (connection *backendRowBaselineConn) SetDeadline(time.Time) error    { return nil }
func (connection *backendRowBaselineConn) SetReadDeadline(time.Time) error { return nil }
func (connection *backendRowBaselineConn) SetWriteDeadline(time.Time) error {
	return nil
}

type backendRowBaselineAddr struct{}

func (backendRowBaselineAddr) Network() string { return "baseline" }
func (backendRowBaselineAddr) String() string  { return "baseline" }
EOF

go test ./hat/hatPgWire/server.go ./hat/hatPgWire/cancel.go ./hat/hatPgWire/metrics.go "$benchmark_file" -run '^$' -bench '^BenchmarkWriteDataRowAllocatingBaseline$' -benchmem -count=1
