#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDataStructure/tuple_format.go hat/hatDataStructure/tuple_format_test.go hat/hatDataStructure/tuple_field_offsets.go hat/hatDataStructure/tuple_field_updates.go
