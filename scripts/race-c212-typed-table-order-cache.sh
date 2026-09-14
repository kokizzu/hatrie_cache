#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestC212TypedTableColumnarOrder' -count=1
