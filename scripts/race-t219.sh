#!/usr/bin/env bash
set -euo pipefail

go test -race -tags=t219 ./hat/hatDataStructure
