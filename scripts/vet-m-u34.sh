#!/usr/bin/env bash
set -euo pipefail

exec go vet ./hat/hatCache
