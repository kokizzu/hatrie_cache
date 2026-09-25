#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatStorage -run '^TestCH025' -count=1
