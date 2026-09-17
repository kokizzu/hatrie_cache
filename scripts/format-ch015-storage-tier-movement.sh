#!/bin/sh
set -eu

gofmt -w hat/hatStorage/ch015_storage_tier_move.go hat/hatStorage/ch015_storage_tier_move_test.go hat/hatStorage/ch015_storage_tier_move_benchmark_test.go
