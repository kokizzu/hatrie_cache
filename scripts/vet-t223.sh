#!/usr/bin/env bash
set -euo pipefail

make vet-tr023-functional-index
go vet ./hat/hatCache ./hat/hatSchema ./hat/hatSql ./hat/hatDataStructure
