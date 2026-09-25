#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatTopology -run 'TestTT001AutomaticBucketRebalance' -count=1
