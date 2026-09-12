#!/usr/bin/env bash
set -eu

gofmt -w \
hat/hatDataStructure/conditional_index.go \
hat/hatDataStructure/conditional_index_test.go \
hat/hatDataStructure/conditional_index_public_test.go
