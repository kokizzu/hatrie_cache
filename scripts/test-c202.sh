#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^TestC202' -count=1
