#!/usr/bin/env bash
set -euo pipefail

GOTOOLCHAIN=auto go vet ./hat/hatSql
