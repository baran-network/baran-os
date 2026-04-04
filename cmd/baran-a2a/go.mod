module github.com/baran-network/baran-os/cmd/baran-a2a

go 1.26.1

require github.com/baran-network/baran-os/a2a v0.0.0-00010101000000-000000000000

require (
	github.com/baran-network/baran-os/core v0.0.0-00010101000000-000000000000 // indirect
	github.com/baran-network/baran-os/protocol v0.0.0-00010101000000-000000000000 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/nats-io/nats.go v1.50.0 // indirect
	github.com/nats-io/nkeys v0.4.15 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.49.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace (
	github.com/baran-network/baran-os/a2a => ../../a2a
	github.com/baran-network/baran-os/core => ../../core
	github.com/baran-network/baran-os/protocol => ../../protocol
	github.com/baran-network/baran-os/sdk => ../../sdk
)
