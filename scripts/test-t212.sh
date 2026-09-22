#!/usr/bin/env bash
set -euo pipefail

go test -tags=t212 ./hat/hatCache -run '^TestT212' -count=1
