#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDataStructure/tuple_field_updates.go hat/hatDataStructure/tuple_field_updates_benchmark_test.go hat/hatDataStructure/tuple_field_updates_test.go hat/hatDataStructure/tuple_field_updates_public_test.go
