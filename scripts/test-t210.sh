#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^TestT210' -count=1
