#!/bin/sh
set -eu

case "${1:-test}" in
test)
	go test ./hat/hatMonitoring -run '^TestReadSchedulerReport$' -count=1
	;;
http)
	go test ./hat/hatCache -run '^TestMonitoring(Scheduler|OpenAPI|Prometheus)' -count=1
	;;
race-http)
	go test -race ./hat/hatCache -run '^TestMonitoring(Scheduler|OpenAPI|Prometheus)' -count=1
	;;
bench)
	go test -v ./hat/hatMonitoring -run '^$' -bench '^BenchmarkReadSchedulerReport$' -benchmem -benchtime=1s -count=3
	;;
race)
	go test -race ./hat/hatMonitoring -run '^TestReadSchedulerReport$' -count=1
	;;
format)
	gofmt -w hat/hatMonitoring/scheduler.go hat/hatMonitoring/scheduler_test.go hat/hatCache/monitoring.go hat/hatCache/monitoring_scheduler_test.go
	;;
*)
	printf '%s\n' 'usage: test-runtime-introspection.sh [test|http|race-http|race|bench|format]' >&2
	exit 2
	;;
esac
