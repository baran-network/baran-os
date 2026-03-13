.PHONY: proto test dev lint clean

proto:
	buf generate

test:
	cd protocol && go test ./...
	cd core && go test ./...

dev:
	nats-server --jetstream --store_dir ./nats-data

lint:
	buf lint

clean:
	rm -rf protocol/gen/go/*
