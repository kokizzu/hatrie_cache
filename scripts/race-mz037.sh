#!/bin/sh
set -eu

go test -race ./hat/hatPipeline -run 'MZ037|AsyncBatcher|PartitionedAsyncBatcher'
