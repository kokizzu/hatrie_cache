#!/usr/bin/env bash
set -euo pipefail

GOCACHE="$PWD/.gocache-vet" go vet ./hat/hatSql
