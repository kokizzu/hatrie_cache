#!/usr/bin/env bash
set -euo pipefail

rg -n \
  'MZ012 Kafka-Style Consumer-Group Fencing|ConsumerGroupFence|make benchmark-mz012-consumer-group-fence' \
  MZ012_CONSUMER_GROUP_FENCING.md
rg -n 'MZ-12.*\[x\].*MZ012_CONSUMER_GROUP_FENCING.md' INSPIRATION_BACKLOG.md
rg -n 'MZ012_CONSUMER_GROUP_FENCING.md|mz-012-kafka-style-consumer-group-fencing' README.md
rg -n '<a id="mz-012-kafka-style-consumer-group-fencing"></a>|## MZ012 Kafka-Style Consumer-Group Fencing' BENCHMARK.md
