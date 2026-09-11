#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestPersistentStoreDiskReserve' -count=1
