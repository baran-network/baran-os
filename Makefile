.PHONY: proto test test-race test-sdk dev lint fmt check clean build demo demo-down demo-smoke

demo:
	scripts/demo/up.sh

demo-down:
	scripts/demo/down.sh

demo-smoke:
	scripts/demo/smoke.sh


proto:
	buf generate

test:
	cd protocol && go test ./...
	cd core && go test ./...
	cd sdk && go test ./...

test-sdk:
	cd sdk && go test -race ./...

test-race:
	cd protocol && go test -race ./...
	cd core && go test -race ./...
	cd sdk && go test -race ./...

build:
	cd core && go build -ldflags "-X main.version=$$(git describe --tags --always 2>/dev/null || echo dev) -X main.commit=$$(git rev-parse --short HEAD)" -o ../baran ./cmd/baran

dev: build
	./baran

lint:
	buf lint
	cd core && golangci-lint run ./...

fmt:
	cd core && go fmt ./...
	cd protocol && go fmt ./...

check: fmt lint test-race

clean:
	rm -rf protocol/gen/go/*
