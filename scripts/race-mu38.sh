#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"
go test -race ./hat/hatStorage -run 'TestMU38' -count=1
