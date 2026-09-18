#!/bin/sh
set -eu
go test ./hat/hatPipeline -run 'TestQueuePartitionOwnership' -count=1
