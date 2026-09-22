#!/usr/bin/env bash
set -euo pipefail

go test -tags=t214 ./hat/hatCache -run '^TestT214'
