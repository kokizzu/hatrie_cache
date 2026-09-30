#!/usr/bin/env bash
set -euo pipefail

go test hat/hatDataStructure/tuple_field_offsets.go \
    hat/hatDataStructure/tuple_format.go \
    hat/hatDataStructure/tuple_format_fields_benchmark_test.go \
    -run '^$' -bench '^BenchmarkTupleFormatFields$' -benchmem -count=10
