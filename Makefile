.PHONY: proto test test-race dev lint fmt check clean

proto:
	buf generate

test:
	cd protocol && go test ./...
	cd core && go test ./...

test-race:
	cd protocol && go test -race ./...
	cd core && go test -race ./...

dev:
	nats-server --jetstream --store_dir ./nats-data

lint:
	buf lint
	cd core && golangci-lint run ./...

fmt:
	cd core && gofmt -l -w .
	cd core && goimports -l -w .

check: fmt lint test-race

clean:
	rm -rf protocol/gen/go/*
