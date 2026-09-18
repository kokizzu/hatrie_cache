#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPipeline -run 'TestMZ010' -count=1
