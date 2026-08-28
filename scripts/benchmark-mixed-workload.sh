#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkCommandFeature/(MixedReadHeavy100|MixedWriteHeavy100)$' -benchmem -count=3
