#!/usr/bin/env bash
set -euo pipefail

go vet ./hat/hatSql ./hat/hatCache
