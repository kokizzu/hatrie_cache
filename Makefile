MONITORING_ADDR ?= 127.0.0.1:8080
MONITORING_WEB_DIR ?= svelte-mpa/dist
MONITORING_TLS_CERT ?=
MONITORING_TLS_KEY ?=
NODE_ID ?=
TOPOLOGY_PATH ?=
ELECTION_TIMEOUT ?= 15s
REPLICATION ?= false
ENFORCE_LEADER_WRITES ?= false
GRPC_ADDR ?=
DB_PATH ?=
DB_SYNC_INTERVAL ?= 0
DB_HOT_LOAD ?= false
DB_HOT_LOAD_MAX_BYTES ?= 1024
DB_HOT_LOAD_MAX_AGE ?= 1h
DB_HOT_LOAD_MIN_HITS ?= 1000
SNAPSHOT_PATH ?=
SNAPSHOT_INTERVAL ?= 0
JOURNAL_PATH ?=

.PHONY: test verify verify-go verify-c verify-frontend bench run generate-proto cli monitoring-server frontend-install frontend-dev frontend-check frontend-test frontend-build

test: verify-go

verify: verify-go verify-c verify-frontend

verify-go:
	./scripts/verify-go.sh

verify-c:
	./scripts/verify-c.sh

verify-frontend:
	./scripts/frontend.sh verify

bench:
	go test -run '^$$' -bench=RawBytes -benchmem

run:
	@CMD='$(CMD)' ./scripts/run.sh

generate-proto:
	./scripts/generate-proto.sh

cli:
	./scripts/cli.sh $(ARGS)

monitoring-server:
	MONITORING_ADDR='$(MONITORING_ADDR)' MONITORING_WEB_DIR='$(MONITORING_WEB_DIR)' MONITORING_TLS_CERT='$(MONITORING_TLS_CERT)' MONITORING_TLS_KEY='$(MONITORING_TLS_KEY)' NODE_ID='$(NODE_ID)' TOPOLOGY_PATH='$(TOPOLOGY_PATH)' ELECTION_TIMEOUT='$(ELECTION_TIMEOUT)' REPLICATION='$(REPLICATION)' ENFORCE_LEADER_WRITES='$(ENFORCE_LEADER_WRITES)' GRPC_ADDR='$(GRPC_ADDR)' DB_PATH='$(DB_PATH)' DB_SYNC_INTERVAL='$(DB_SYNC_INTERVAL)' DB_HOT_LOAD='$(DB_HOT_LOAD)' DB_HOT_LOAD_MAX_BYTES='$(DB_HOT_LOAD_MAX_BYTES)' DB_HOT_LOAD_MAX_AGE='$(DB_HOT_LOAD_MAX_AGE)' DB_HOT_LOAD_MIN_HITS='$(DB_HOT_LOAD_MIN_HITS)' SNAPSHOT_PATH='$(SNAPSHOT_PATH)' SNAPSHOT_INTERVAL='$(SNAPSHOT_INTERVAL)' JOURNAL_PATH='$(JOURNAL_PATH)' ./scripts/monitoring-server.sh

frontend-install:
	./scripts/frontend.sh install

frontend-dev:
	./scripts/frontend.sh dev

frontend-check:
	./scripts/frontend.sh check

frontend-test:
	./scripts/frontend.sh test

frontend-build:
	./scripts/frontend.sh build
