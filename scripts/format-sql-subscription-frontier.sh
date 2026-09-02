#!/bin/sh
set -eu

gofmt -w hat/hatSql/subscription.go hat/hatSql/subscription_frontier_test.go
