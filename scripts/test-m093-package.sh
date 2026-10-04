#!/usr/bin/env bash
set -euo pipefail

export GOCACHE=/tmp/hatrie-next-m093-go-cache
go test ./hat/hatPipeline -count=1
