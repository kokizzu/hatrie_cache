#!/usr/bin/env bash
set -euo pipefail

go test -tags t210 ./hat/hatReplication -run '^TestT210' -count=1
