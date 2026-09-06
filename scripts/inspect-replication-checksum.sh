#!/usr/bin/env bash
set -euo pipefail
rg -n -A 32 'Checksum|checksum|Merkle|merkle|Hash|hash' hat/hatMerkle hat/hatReplication
