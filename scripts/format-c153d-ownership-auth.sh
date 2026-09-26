#!/bin/sh
set -eu

gofmt -w \
	hat/hatTopology/c153d_partition_ownership_auth.go \
	hat/hatTopology/c153d_partition_ownership_auth_test.go \
	hat/hatTopology/ownership_consensus.go
