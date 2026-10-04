#!/bin/sh
set -eu

gofmt -w \
	hat/hatReplication/conflict_decision_log.go \
	hat/hatReplication/conflict_decision_log_test.go
