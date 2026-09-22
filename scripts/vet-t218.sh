#!/usr/bin/env bash
set -euo pipefail

go vet -tags=t218 ./hat/hatDataStructure
