#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '^(TestIncrementalFrameWindow|ExampleNewIncrementalFrameWindow)$' -count=1
