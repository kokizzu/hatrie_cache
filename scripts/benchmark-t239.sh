#!/bin/sh
set -eu

go test ./hat/hatPeer -run '^$' -bench '^(BenchmarkCompactProtocolMarshal|BenchmarkCompactRequestTemplateMarshal)$' -benchmem -count=5
