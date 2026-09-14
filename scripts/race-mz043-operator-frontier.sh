#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatMetrics ./hat/hatCache
