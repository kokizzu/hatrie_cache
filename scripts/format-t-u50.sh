#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
gofmt -w hat/hatTopology/config_watch.go hat/hatTopology/config_watch_test.go
