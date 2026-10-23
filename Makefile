BIN     := bin/hammerdown
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

# keep in sync with .github/workflows/ci.yml
GOLANGCI_VERSION := 2.15
GOLANGCI         := $(shell command -v golangci-lint 2>/dev/null)

.DEFAULT_GOAL := help
.PHONY: help build test fuzz lint fmt check-linter demo demo-failfast demo-play demo-record clean

help: ## list available targets
	@echo "hammerdown - make targets:"
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

build: ## build bin/hammerdown
	go build -ldflags "-X main.version=$(VERSION)" -o $(BIN) ./cmd/hammerdown

test: ## go vet + race tests
	go vet ./...
	go test -race ./...

fuzz: ## fuzz the --limit/--skip parser for 30s
	go test -run='^$$' -fuzz=FuzzParseSpec -fuzztime=30s ./filter

lint: check-linter ## golangci-lint: formatting + linters (same as CI)
	golangci-lint run ./...

fmt: check-linter ## rewrite code into canonical format (gofumpt + goimports)
	golangci-lint fmt ./...

check-linter:
	@test -n "$(GOLANGCI)" || { echo "golangci-lint not found. Install v$(GOLANGCI_VERSION).x without touching your Go:"; \
		echo '  curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b "$$(go env GOPATH)/bin" v$(GOLANGCI_VERSION).0'; exit 1; }
	@golangci-lint --version | grep -Eq 'version v?$(GOLANGCI_VERSION)\.' || \
		echo "warning: CI uses golangci-lint v$(GOLANGCI_VERSION).x; you have: $$(golangci-lint --version)"

demo: build ## 11k-node sim: init, stage, canary, full roll, report
	$(BIN) sim init --force
	$(BIN) sim stage --to v20.0.1
	$(BIN) report | head -40
	$(BIN) roll --shards-per-keyspace 1 --dry-run=false --yes --quiet
	$(BIN) roll --workers 64 --dry-run=false --yes --quiet 2>/dev/null
	$(BIN) report | tail -24

demo-failfast: build ## fail-fast on a bad failover, then resume
	$(BIN) sim init --force
	$(BIN) sim stage --to v20.0.1
	-$(BIN) roll --workers 64 --sim-cursed 2 --dry-run=false --yes --quiet 2>&1 | grep -E 'FAIL-FAST|primary failure:' | head -5
	$(BIN) roll --workers 64 --dry-run=false --yes --quiet 2>/dev/null | tail -9

demo-play: build ## paced, narrated demo (what the recording shows; PAUSE=1 to speed up)
	scripts/demo.sh

demo-record: build ## record demo-play: docs/demo.cast -> demo.gif (README) -> demo.mp4 (LinkedIn)
	mkdir -p docs
	asciinema rec --overwrite --idle-time-limit 2 -c scripts/demo.sh docs/demo.cast
	agg --speed 1.5 --font-size 16 docs/demo.cast docs/demo.gif
	ffmpeg -y -loglevel error -i docs/demo.gif -movflags faststart -pix_fmt yuv420p -vf "scale=trunc(iw/2)*2:trunc(ih/2)*2" docs/demo.mp4
	@ls -lh docs/demo.*

clean: ## remove bin/ and the sim fleet
	rm -rf bin .hammerdown
