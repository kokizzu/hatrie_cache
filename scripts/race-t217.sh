#!/usr/bin/env bash
set -euo pipefail

go test -race -tags=t217 ./hat/hatDataStructure
