#!/usr/bin/env bash
set -euo pipefail

go vet -tags=t219 ./hat/hatDataStructure
