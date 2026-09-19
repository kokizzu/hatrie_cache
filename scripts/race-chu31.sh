#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatStorage -run '^TestCHU31' -count=1
