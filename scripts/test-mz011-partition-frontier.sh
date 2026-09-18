#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^TestMZ011PartitionOffsetFrontier' -count=1
