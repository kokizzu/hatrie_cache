#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run 'TestTT001AutomaticBucketRebalance' -count=1
