#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^TestMZ004FrontierCompactionScheduler' -count=1
