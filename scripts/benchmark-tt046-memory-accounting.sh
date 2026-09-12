#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkTT046Memory' -benchmem -count=5
