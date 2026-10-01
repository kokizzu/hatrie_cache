#!/usr/bin/env bash
set -euo pipefail

mode=${1:-test}
case "$mode" in
	format)
		gofmt -w hat/hatMetrics/source_health.go hat/hatMetrics/source_health_test.go
		;;
	test)
		go test ./hat/hatMetrics -run 'TestSourceHealthRegistry' -count=1
		;;
	benchmark)
		go test ./hat/hatMetrics -run '^$' -bench 'BenchmarkSourceHealth' -benchmem -count=5
		;;
	race)
		go test -race ./hat/hatMetrics -run 'TestSourceHealthRegistry' -count=1
		;;
	vet)
		go vet ./hat/hatMetrics
		;;
	*)
		printf 'usage: %s format|test|benchmark|race|vet\n' "$0" >&2
		exit 2
		;;
esac
