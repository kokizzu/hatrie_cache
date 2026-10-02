#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkVolatileEngineReadWrite/' -benchmem -count=5 -benchtime=500x
