#!/bin/sh
set -eu

go test -race ./hat/hatPipeline -run 'TestDataflowGraph' -count=1
