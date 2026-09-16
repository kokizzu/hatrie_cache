#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPipeline \
	-run '^TestMZ007FrontierBackpressure' \
	-count=1
