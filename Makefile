MONITORING_ADDR ?= 127.0.0.1:8080
MONITORING_WEB_DIR ?= svelte-mpa/dist
MONITORING_TLS_CERT ?=
MONITORING_TLS_KEY ?=
GRPC_ADDR ?=
DB_PATH ?=
DB_SYNC_INTERVAL ?= 0
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
	MONITORING_ADDR='$(MONITORING_ADDR)' MONITORING_WEB_DIR='$(MONITORING_WEB_DIR)' MONITORING_TLS_CERT='$(MONITORING_TLS_CERT)' MONITORING_TLS_KEY='$(MONITORING_TLS_KEY)' GRPC_ADDR='$(GRPC_ADDR)' DB_PATH='$(DB_PATH)' DB_SYNC_INTERVAL='$(DB_SYNC_INTERVAL)' SNAPSHOT_PATH='$(SNAPSHOT_PATH)' SNAPSHOT_INTERVAL='$(SNAPSHOT_INTERVAL)' JOURNAL_PATH='$(JOURNAL_PATH)' ./scripts/monitoring-server.sh

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
