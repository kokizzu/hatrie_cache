#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline
go test -race ./hat/hatPipeline
go vet ./hat/hatPipeline
