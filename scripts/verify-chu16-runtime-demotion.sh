#!/bin/sh
set -eu

sh ./scripts/format-chu16-runtime-demotion.sh
sh ./scripts/test-chu16-runtime-demotion.sh
sh ./scripts/race-chu16-runtime-demotion.sh
sh ./scripts/vet-chu16-runtime-demotion.sh
