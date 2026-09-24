#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -count=1
go test -race ./hat/hatPipeline -run 'TestMZ002|TestFrontierRegistry' -count=1
go vet ./hat/hatPipeline
