#!/usr/bin/env bash
set -euo pipefail

if [[ -n "$(gofmt -d hat/hatDataStructure/ordered_index.go hat/hatDataStructure/ordered_index_benchmark_test.go hat/hatDataStructure/ordered_index_test.go)" ]]; then
	echo "ordered index files are not gofmt-formatted" >&2
	exit 1
fi
go test ./hat/hatDataStructure -run '^TestOrderedIndex' -count=1
go test -race ./hat/hatDataStructure -run '^TestOrderedIndex' -count=1
go vet ./hat/hatDataStructure
