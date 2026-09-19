#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^TestCHU28' -count=1
