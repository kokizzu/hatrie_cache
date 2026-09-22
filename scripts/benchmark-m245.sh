#!/usr/bin/env bash
set -euo pipefail

baseline=$(mktemp -d /tmp/hatrie-m245-baseline.XXXXXX)
cleanup() {
  git worktree remove --force "$baseline"
}
trap cleanup EXIT

git worktree add --detach "$baseline" HEAD
cat > "$baseline/hat/hatSql/m245_parent_benchmark_tmp_test.go" <<'EOF'
package hatSql

import "testing"

var m245ParentTelemetrySink *SQLTelemetry

func BenchmarkM245QueryTelemetryObserve(b *testing.B) {
	telemetry := NewSQLTelemetry()
	event := SQLQueryEvent{ElapsedNanos: 1000, ResultBytes: 64, OK: true}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		telemetry.ObserveSQLQuery(event)
	}
	m245ParentTelemetrySink = telemetry
}
EOF

printf '%s\n' '== M244 parent baseline: existing query observer =='
(cd "$baseline" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkM245QueryTelemetryObserve$' -benchmem -count=5)
printf '%s\n' '== M245 current: existing observer and timestamp observer =='
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM245(Query|Timestamp)' -benchmem -count=5
