#!/usr/bin/env bash
set -euo pipefail

go vet ./hat/hatTrace ./hat/hatSql
