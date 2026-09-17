#!/bin/sh
set -eu

go test ./hat/hatStorage -run '^$' -bench '^BenchmarkCH015StorageTierSelectBaseline$' -benchmem -benchtime=250ms -count=5
