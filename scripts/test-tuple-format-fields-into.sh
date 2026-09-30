#!/usr/bin/env bash
set -euo pipefail

go test hat/hatDataStructure/tuple_field_offsets.go \
    hat/hatDataStructure/tuple_format.go \
    hat/hatDataStructure/tuple_format_fields_into_test.go
go test -race hat/hatDataStructure/tuple_field_offsets.go \
    hat/hatDataStructure/tuple_format.go \
    hat/hatDataStructure/tuple_format_fields_into_test.go
