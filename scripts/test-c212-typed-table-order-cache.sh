#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestC212TypedTableColumnarOrder' -count=1
