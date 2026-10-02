#!/usr/bin/env bash
set -euo pipefail

mode="${1:?expected format, baseline, red, unit, race, vet, package, or benchmark}"
tmp_dir="$(mktemp -d /tmp/hatrie-cache-chg22-test-XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT

export GOCACHE="$tmp_dir/go-build"

case "$mode" in
format)
	gofmt -w hat/hatReplication/space_changefeed.go hat/hatReplication/space_changefeed_test.go hat/hatReplication/space_changefeed_baseline_benchmark_test.go
	;;
baseline)
	baseline_dir="$tmp_dir/baseline"
	mkdir -p "$baseline_dir"
	printf '%s\n' \
		'package baseline' \
		'' \
		'import "testing"' \
		'' \
		'type event struct {' \
		'    sequence uint64' \
		'    key, value []byte' \
		'}' \
		'' \
		'type log struct { events []event }' \
		'' \
		'func (l *log) append(e event) {' \
		'    e.sequence = uint64(len(l.events) + 1)' \
		'    l.events = append(l.events, e)' \
		'}' \
		'' \
		'func BenchmarkLegacyChangeLogAppend(b *testing.B) {' \
		'    var l log' \
		'    e := event{key: []byte("order:42"), value: []byte("value")}' \
		'    b.ReportAllocs()' \
		'    b.ResetTimer()' \
		'    for i := 0; i < b.N; i++ {' \
		'        l.append(e)' \
		'    }' \
		'}' > "$baseline_dir/changefeed_test.go"
	(
		cd "$baseline_dir"
		GO111MODULE=off go test -run '^$' -bench 'LegacyChangeLogAppend' -benchmem -benchtime=100ms -count=1
	)
	;;
red)
	go test ./hat/hatReplication -run 'TestSpaceChangefeed' -count=1
	;;
unit)
	go test ./hat/hatReplication -run 'TestSpaceChangefeed' -count=1
	;;
race)
	go test -race ./hat/hatReplication -run 'TestSpaceChangefeed' -count=1
	;;
vet)
	go vet ./hat/hatReplication
	;;
package)
	go test ./hat/hatReplication -count=1
	;;
benchmark)
	go test ./hat/hatReplication -run '^$' -bench 'LegacyChangeLogAppend|SpaceChangefeed' -benchmem -benchtime=100ms -count=1
	;;
*)
	printf 'unknown mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
