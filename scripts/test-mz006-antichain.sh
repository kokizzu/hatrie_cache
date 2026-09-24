#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^TestMZ006' -count=1
