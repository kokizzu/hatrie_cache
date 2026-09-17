#!/bin/sh
set -eu

sh ./scripts/review-chu16-adaptive-dictionary.sh
sh ./scripts/stage-chu16-adaptive-dictionary.sh
sh ./scripts/review-chu16-adaptive-dictionary.sh
sh ./scripts/commit-chu16-adaptive-dictionary.sh
sh ./scripts/push-chu16-adaptive-dictionary.sh
git status --short
git log -1 --oneline
