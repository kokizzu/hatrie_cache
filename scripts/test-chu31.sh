#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^TestCHU31' -count=1
