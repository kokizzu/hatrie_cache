#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/graph.go hat/hatSql/graph_test.go
