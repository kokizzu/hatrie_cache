#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSpill -count=1
