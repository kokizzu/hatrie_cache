#!/usr/bin/env bash
set -eu

gofmt -w \
	hat/hatDataStructure/tuple_field_offsets.go \
	hat/hatDataStructure/tuple_field_offsets_test.go \
	hat/hatDataStructure/tuple_field_offsets_public_test.go
