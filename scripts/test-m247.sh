#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^TestM247' -count=1
