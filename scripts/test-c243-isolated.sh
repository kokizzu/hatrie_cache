#!/usr/bin/env bash
set -euo pipefail

go test \
	./hat/hatStorage/remote_part.go \
	./hat/hatStorage/remote_part_cache.go \
	./hat/hatStorage/remote_part_cache_c243_test.go \
	-count=1
