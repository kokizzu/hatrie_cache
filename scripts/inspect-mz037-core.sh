#!/bin/sh
set -eu

rg -n 'MZ-037|MZ-37|exchange batching|exchange|type .*Batch|func .*Batch|BatchSize|BatchWindow' ENGINE_IDEAS.md INSPIRATION_BACKLOG.md hat/hatPipeline/*.go hat/hatSql/m052*.go hat/hatSql/dataflow*.go
