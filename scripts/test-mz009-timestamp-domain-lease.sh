#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^TestMZ009' -count=1
