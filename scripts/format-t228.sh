#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
	hat/hatDataStructure/tuple_format_reader.go \
	hat/hatDataStructure/t228_tuple_reader_baseline_test.go \
	hat/hatDataStructure/t228_tuple_format_compatibility_test.go
