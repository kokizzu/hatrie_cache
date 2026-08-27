#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
source="$root/hat/hatCache"

report() {
    title=$1
    pattern=$2
    printf '\n== %s ==\n' "$title"
    rg -l "$pattern" "$source" --glob '*.go' --glob '!*_test.go'
}

count=0
production_count=0
for file in "$source"/*.go; do
    if [ -f "$file" ]; then
        count=$((count + 1))
        case "$file" in
        *_test.go) ;;
        *) production_count=$((production_count + 1)) ;;
        esac
    fi
done
printf 'hatCache Go source files: %s\n' "$count"
printf 'hatCache production Go files: %s\n' "$production_count"

report 'Storage, persistence, backup, and recovery' 'LevelDB|Pebble|Snapshot|Backup|Restore|Journal|Compaction|spill'
report 'Replication, topology, and partitioning' 'Replication|Replica|Peer|Topology|Election|Partition|Quorum'
report 'Monitoring and network management' 'Monitoring|GRPC|HTTP2|Metrics|Health|Admin'
report 'SQL execution and relational cache adapters' 'SQL|Query|Relation|Schema|Index'
report 'Data-structure commands and adapters' 'Bloom|Cuckoo|HyperLogLog|CountMin|Priority|Queue|Stack|Roaring|Reservoir|Quantile|TopK|Radix'
