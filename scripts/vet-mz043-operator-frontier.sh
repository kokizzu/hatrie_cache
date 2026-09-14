#!/usr/bin/env bash
set -euo pipefail

go vet ./hat/hatMetrics ./hat/hatCache
