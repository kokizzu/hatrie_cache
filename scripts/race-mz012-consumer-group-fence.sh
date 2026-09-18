#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPipeline -run '^TestMZ012ConsumerGroupFence' -count=1
