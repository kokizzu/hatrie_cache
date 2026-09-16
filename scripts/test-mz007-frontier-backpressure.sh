#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline \
	-run '^TestMZ007FrontierBackpressure' \
	-count=1
