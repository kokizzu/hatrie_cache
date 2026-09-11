#!/usr/bin/env bash
set -euo pipefail

rg -n 'CH-049|external dictionary|DICT_GET|DICT_HAS' SQL_EXTERNAL_DICTIONARIES.md ADOPTED_QUERY_ENGINE_IDEAS.md ENGINE_IDEAS.md README.md BENCHMARK.md
