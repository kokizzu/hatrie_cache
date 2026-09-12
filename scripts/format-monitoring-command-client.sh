#!/bin/sh
set -eu

gofmt -w hat/hatMonitoring/client.go hat/hatMonitoring/client_command_test.go hat/hatMonitoring/client_command_benchmark_test.go
