#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatIndexStats -count=1
