#!/bin/sh
set -eu

gofmt -w \
	hat/hatSql/logical_short_circuit.go \
	hat/hatSql/logical_short_circuit_test.go \
	hat/hatSql/logical_short_circuit_benchmark_test.go
