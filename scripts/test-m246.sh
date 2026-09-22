#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^TestM246' -count=1
