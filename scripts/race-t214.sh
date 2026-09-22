#!/usr/bin/env bash
set -euo pipefail

go test -race -tags=t214 ./hat/hatCache -run '^TestT214'
