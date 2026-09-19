#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatStorage -run '^TestCHU32' -count=1
