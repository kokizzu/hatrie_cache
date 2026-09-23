#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
	hat/hatDataStructure/space.go \
	 hat/hatDataStructure/t229_space_before_replace_baseline_test.go \
	 hat/hatDataStructure/t229_space_before_replace_test.go
