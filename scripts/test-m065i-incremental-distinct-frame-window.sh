#!/usr/bin/env bash
set -eu

go test ./hat/hatSql \
	-run '^(TestIncrementalFrameWindowCountDistinct|ExampleIncrementalFrameWindow_countDistinct)$' \
	-count=1
