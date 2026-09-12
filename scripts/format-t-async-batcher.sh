#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPipeline/async_batcher.go hat/hatPipeline/async_batcher_test.go
