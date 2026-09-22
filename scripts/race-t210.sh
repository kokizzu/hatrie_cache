#!/usr/bin/env bash
set -euo pipefail

go test -race -tags t210 ./hat/hatReplication -count=1
