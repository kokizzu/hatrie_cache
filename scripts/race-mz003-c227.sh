#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPipeline -run '^TestMZ003' -count=1
