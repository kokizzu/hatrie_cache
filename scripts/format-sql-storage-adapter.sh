#!/bin/sh
set -eu

gofmt -w ./hat/hatStorage/sql_adapter.go ./hat/hatStorage/sql_adapter_test.go
