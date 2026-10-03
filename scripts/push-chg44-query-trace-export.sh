#!/usr/bin/env bash
set -euo pipefail

git push -u origin "$(git branch --show-current)"
