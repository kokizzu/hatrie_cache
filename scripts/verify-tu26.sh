#!/usr/bin/env bash
set -euo pipefail

make format-tu26
make test-tu26
make race-tu26
make vet-tu26
