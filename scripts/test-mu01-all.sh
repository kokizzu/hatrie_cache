#!/bin/sh
set -eu

sh scripts/test-mu01.sh
sh scripts/test-mu01-package.sh
sh scripts/test-race-mu01.sh
sh scripts/vet-mu01.sh
sh scripts/verify-mu01-docs.sh
