#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatStorage -run '^TestCHU28' -count=1
