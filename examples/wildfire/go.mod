module github.com/baran-network/baran-os/examples/wildfire

go 1.26.1

require google.golang.org/protobuf v1.36.11

replace (
	github.com/baran-network/baran-os/core => ../../core
	github.com/baran-network/baran-os/protocol => ../../protocol
	github.com/baran-network/baran-os/sdk => ../../sdk
)
