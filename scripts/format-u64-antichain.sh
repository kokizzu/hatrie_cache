#!/usr/bin/env bash
set -eu

gofmt -w \
	hat/hatDataStructure/u64_antichain.go \
	hat/hatDataStructure/u64_antichain_test.go \
	hat/hatDataStructure/u64_antichain_public_test.go
