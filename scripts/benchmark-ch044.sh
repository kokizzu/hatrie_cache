#!/bin/sh
set -eu
go test ./hat/hatCache -run '^$' -bench '^BenchmarkCH044' -benchmem -count=5
