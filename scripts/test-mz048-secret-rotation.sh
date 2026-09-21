#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^TestMZ048' -count=1
