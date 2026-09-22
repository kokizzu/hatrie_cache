#!/usr/bin/env bash
set -euo pipefail

go test -tags t210 ./hat/hatReplication -count=1
