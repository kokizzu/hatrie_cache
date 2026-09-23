#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -count=1
