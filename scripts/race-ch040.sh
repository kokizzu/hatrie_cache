#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatDataStructure ./hat/hatSql -count=1
