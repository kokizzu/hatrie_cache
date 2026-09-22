#!/usr/bin/env bash
set -euo pipefail

go test -race -tags=t216 ./hat/hatDataStructure
