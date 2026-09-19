#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^TestCHU32' -count=1
