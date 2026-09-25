#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^TestCH021' -count=1
