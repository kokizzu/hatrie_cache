#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatStorage -run '^TestCH020' -count=1
