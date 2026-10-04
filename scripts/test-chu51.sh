#!/usr/bin/env sh
set -eu

mode=${1:-test}
case "$mode" in
red)
	go test ./hat/hatCache -run '^TestMonitoringSQLQueryLogFlush$' -count=1
	;;
test)
	go test ./hat/hatCache -run '^TestMonitoringSQLQueryLogFlush' -count=1
	;;
race)
	go test -race ./hat/hatCache -run '^TestMonitoringSQLQueryLogFlush' -count=1
	;;
vet)
	go vet ./hat/hatCache
	;;
format)
	gofmt -w ./hat/hatCache/chu51_query_log_flush_test.go ./hat/hatCache/monitoring.go ./hat/hatCache/monitoring_sql_query_log.go
	;;
benchmark)
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH031SQLQueryLogAppend$' -benchmem -count=5
	go test ./hat/hatCache -run '^$' -bench '^BenchmarkCHU51MonitoringSQLQueryLogFlush$' -benchmem -count=5
	;;
package)
	go test ./hat/hatCache -count=1
	go test ./hat/hatSql -count=1
	;;
*)
	printf 'unknown test-chu51 mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
