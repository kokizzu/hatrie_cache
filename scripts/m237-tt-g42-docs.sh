#!/usr/bin/env bash
set -euo pipefail

rg -n '^## TT-G42 Per-Peer Adaptive Flow Control$|TTG42_PEER_FLOW_CONTROL|NewPeerRelayBackpressure|TT-G42' BENCHMARK.md README.md IDEA_GAP_CATALOG.md TTG42_PEER_FLOW_CONTROL.md
