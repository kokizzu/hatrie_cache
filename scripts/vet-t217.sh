#!/usr/bin/env bash
set -euo pipefail

go vet -tags=t217 ./hat/hatDataStructure
