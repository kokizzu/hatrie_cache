#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSql/mutation_workload_test.go ./hat/hatCache/seeded_workload_test.go
