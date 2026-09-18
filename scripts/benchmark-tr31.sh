#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkTR031(AutomaticIndexChoice|AdaptiveFeedback)$' -benchmem -count=5
