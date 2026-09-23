#!/usr/bin/env bash
set -euo pipefail

rg '^\| CH-[A-Z0-9-]+' IDEA_GAP_CATALOG.md
