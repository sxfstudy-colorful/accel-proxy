BINARY  := accel-proxy
CMD     := ./cmd/proxy
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -X main.version=$(VERSION) -s -w

.PHONY: all build test lint clean run-access run-relay run-egress

all: build

## build: compile the binary
build:
	go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY) $(CMD)

## test: run all tests
test:
	go test ./... -v -race -timeout 60s

## lint: run golangci-lint
lint:
	golangci-lint run ./...

## tidy: tidy go modules
tidy:
	go mod tidy

## clean: remove build artifacts
clean:
	rm -rf bin/

## run-access: run an access node with example config
run-access: build
	./bin/$(BINARY) -config configs/access.yaml -log-level debug

## run-relay: run a relay node with example config
run-relay: build
	./bin/$(BINARY) -config configs/relay.yaml -log-level debug

## run-egress: run an egress node with example config
run-egress: build
	./bin/$(BINARY) -config configs/egress.yaml -log-level debug

## docker-build: build a minimal Docker image
docker-build:
	docker build -t accel-proxy:$(VERSION) .

help:
	@grep -E '^##' Makefile | sed 's/## //'
