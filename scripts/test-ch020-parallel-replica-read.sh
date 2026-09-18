#!/usr/bin/env bash
set -eu
go test ./hat/hatPipeline -run 'TestCH020ParallelReplicaReads' -count=1
