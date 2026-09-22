#!/usr/bin/env bash
set -euo pipefail

go test -race -tags=t213 ./hat/hatCache -run '^TestT213'
