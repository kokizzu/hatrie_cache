#!/usr/bin/env bash
set -eu

go test -race ./hat/hatSql -run '^(TestIncrementalFrameWindowDistinctOutOfOrderBatchIsAtomic|TestIncrementalFrameWindowCountDistinct)' -count=1
