#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache ./hat/hatStorage -count=1
