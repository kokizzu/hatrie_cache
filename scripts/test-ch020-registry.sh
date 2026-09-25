#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^TestCH020' -count=1
