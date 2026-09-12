#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkGRPCCommandStreamRequestIDs$' -benchmem -benchtime=1s -count=5
