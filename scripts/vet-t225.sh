#!/usr/bin/env bash
set -euo pipefail

make vet-tr024-covering-index
go vet ./hat/hatCache ./hat/hatSchema ./hat/hatSql
