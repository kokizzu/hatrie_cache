#!/bin/sh
set -eu

go test ./hat/hatMonitoring -run '^$' -bench 'Benchmark(Client|Manual)(Command|Batch)JSON' -benchmem -count=5
