#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSchema -run '^TestTT024TextIndexCatalog'
