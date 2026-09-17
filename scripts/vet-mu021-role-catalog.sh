#!/usr/bin/env bash
set -euo pipefail

exec env GOMAXPROCS=1 go vet ./hat/hatAuth
