#!/usr/bin/env bash
set -euo pipefail

go test -race -run '^TestT044' ./hat/hatMetrics
