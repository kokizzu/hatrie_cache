#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPipeline -run 'TestMZ038' -count=1
