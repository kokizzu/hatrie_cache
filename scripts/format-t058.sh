#!/bin/sh
set -eu

gofmt -w \
	hat/hatReplication/quorum_policy_read.go \
	hat/hatReplication/quorum_policy_read_test.go \
	hat/hatReplication/quorum_policy_read_edge_test.go
