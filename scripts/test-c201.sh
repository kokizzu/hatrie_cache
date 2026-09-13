#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^TestC201' -count=1
