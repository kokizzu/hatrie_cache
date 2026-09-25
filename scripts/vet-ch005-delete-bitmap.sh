#!/usr/bin/env bash
set -euo pipefail

go vet ./hat/hatMerkle ./hat/hatSql
