#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPipeline/pipeline.go hat/hatPipeline/pipeline_test.go
