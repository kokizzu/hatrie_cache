#!/bin/sh
set -eu

go test ./hat/hatPipeline -run '^$' -bench 'Frontier.*Snapshot' -benchmem -count=5 -benchtime=100ms
