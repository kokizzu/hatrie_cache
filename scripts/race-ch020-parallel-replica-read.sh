#!/usr/bin/env bash
set -eu
go test -race ./hat/hatPipeline -run 'TestCH020ParallelReplicaReads' -count=1
