#!/bin/sh
set -eu

gofmt -w \
    hat/hatReplication/tu47_cluster_write_commit_participant_store.go \
    hat/hatReplication/tu47_cluster_write_commit_participant_store_test.go
