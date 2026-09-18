#!/bin/sh
set -eu
go test -race ./hat/hatPipeline -run 'TestQueuePartitionOwnership' -count=1
