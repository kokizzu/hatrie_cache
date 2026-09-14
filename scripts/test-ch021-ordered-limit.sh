#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestCH021OrderedLimitStopsMaterializedQueryAtLimit$' -count=1
