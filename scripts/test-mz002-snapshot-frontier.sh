#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run 'TestMZ002' -count=1
