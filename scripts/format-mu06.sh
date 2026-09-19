#!/bin/sh
set -eu

gofmt -w hat/hatSql/differential_window.go hat/hatSql/m_u06_differential_window_test.go hat/hatSql/m_u06_differential_window_benchmark_test.go
