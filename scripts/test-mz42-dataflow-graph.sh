#!/bin/sh
set -eu

go test ./hat/hatPipeline -run 'TestDataflowGraph' -count=1
