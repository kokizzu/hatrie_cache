#!/bin/sh
set -eu

go test ./hat/hatMonitoring -run '^$' -bench 'Benchmark(Client|Manual)CommandJSON$' -benchmem -count=5
