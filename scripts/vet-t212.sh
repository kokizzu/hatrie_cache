#!/usr/bin/env bash
set -euo pipefail

go vet -tags=t212 ./hat/hatJournal ./hat/hatCache
