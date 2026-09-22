#!/usr/bin/env bash
set -euo pipefail

go vet -tags=t216 ./hat/hatDataStructure
