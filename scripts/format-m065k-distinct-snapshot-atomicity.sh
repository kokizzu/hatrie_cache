#!/usr/bin/env bash
set -eu

gofmt -w \
	hat/hatSql/incremental_frame_window.go \
	hat/hatSql/m065k_distinct_snapshot_atomicity_test.go
