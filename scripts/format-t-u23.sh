#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDataStructure/tuple_multikey_index.go hat/hatDataStructure/t_u23_tuple_multikey_index_test.go hat/hatDataStructure/t_u23_tuple_multikey_index_benchmark_test.go hat/hatDataStructure/t_u23_tuple_multikey_baseline_test.go
