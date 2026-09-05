#!/bin/sh
set -eu

gofmt -w hat/hatSql/subscription_status.go hat/hatSql/subscription_status_test.go
