#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^TestTT024MaterializedTextIndex' -count=1
