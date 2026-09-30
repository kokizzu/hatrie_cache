#!/usr/bin/env bash
set -euo pipefail

go test hat/hatDataStructure/roaring.go hat/hatDataStructure/roaring_values_into_test.go
go test -race hat/hatDataStructure/roaring.go hat/hatDataStructure/roaring_values_into_test.go
