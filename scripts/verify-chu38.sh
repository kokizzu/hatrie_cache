#!/bin/sh
set -eu

sh scripts/verify-chu38-docs.sh
sh scripts/test-chu38.sh
sh scripts/test-chu38-package.sh
sh scripts/test-race-chu38.sh
sh scripts/vet-chu38.sh
