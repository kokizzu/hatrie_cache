#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '(^TestIncrementalFrameWindowExtrema|^TestIncrementalFrameWindowMinMax|^ExampleIncrementalFrameWindow_minMax)$' -count=1
