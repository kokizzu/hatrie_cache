#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPipeline -run '^TestMZ009' -count=1
