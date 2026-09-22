#!/usr/bin/env bash
set -euo pipefail

go vet -tags=t214 ./hat/hatCache
