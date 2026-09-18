#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run 'TestMZ010' -count=1
