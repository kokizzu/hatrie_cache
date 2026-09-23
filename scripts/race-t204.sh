#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatTopology -count=1
