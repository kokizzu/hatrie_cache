#!/usr/bin/env bash
set -euo pipefail

go vet -tags=t215 ./hat/hatDataStructure
