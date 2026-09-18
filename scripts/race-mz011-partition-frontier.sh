#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPipeline -run '^TestMZ011PartitionOffsetFrontier' -count=1
