#!/bin/sh
set -eu

gofmt -w hat/hatCommand/command.go hat/hatCommand/wire.go hat/hatCommand/insert_quorum_wire_test.go hat/hatCache/command.go hat/hatCache/grpc.go hat/hatCache/monitoring.go hat/hatCache/insert_quorum_test.go hat/hatCache/insert_quorum_benchmark_test.go
