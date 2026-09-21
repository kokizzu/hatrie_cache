#!/usr/bin/env bash
set -euo pipefail

bash scripts/stage-tt025-online-uniqueness.sh
git commit -m 'hatSchema: add online unique index validation'
