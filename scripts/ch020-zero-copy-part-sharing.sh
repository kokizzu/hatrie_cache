#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

mode="${1:-verify}"
case "$mode" in
format)
	gofmt -w hat/hatMerkle/ch020_zero_copy_part_sharing.go hat/hatMerkle/ch020_zero_copy_part_sharing_benchmark_test.go hat/hatMerkle/ch020_zero_copy_part_sharing_test.go
	;;
test)
	go test ./hat/hatMerkle -run '^TestCH020SharedPartRegistry'
	;;
package)
	go test ./hat/hatMerkle
	;;
race)
	go test -race ./hat/hatMerkle -run '^TestCH020SharedPartRegistry'
	;;
race-package)
	go test -race ./hat/hatMerkle
	;;
benchmark)
	go test ./hat/hatMerkle -run '^$' -bench '^BenchmarkCH020' -benchmem -count=5
	;;
vet)
	go vet ./hat/hatMerkle
	;;
verify)
	bash "$0" format
	bash "$0" test
	bash "$0" package
	bash "$0" race
	bash "$0" vet
	;;
*)
	printf 'unknown ch020 mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
