#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema ./hat/hatSql ./hat/hatCache -count=1
