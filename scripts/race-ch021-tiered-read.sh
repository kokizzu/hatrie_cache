#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatStorage -run '^TestCH021' -count=1
