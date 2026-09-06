#!/bin/sh
set -eu

go test ./hat/hatBackup \
	-run '^$' \
	-bench '^BenchmarkObjectStoreTargetBackup$' \
	-benchmem \
	-count=5
