#!/bin/sh
set -eu

go test -run '^TestMonitoringOperatorMemoryMetrics' ./hat/hatCache
