#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatFiber -run '^TestT236'
