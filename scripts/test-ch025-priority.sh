#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^TestCH025' -count=1
