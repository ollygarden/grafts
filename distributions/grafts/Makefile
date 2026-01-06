.PHONY: build install-ocb run clean

OCB_VERSION ?= 0.143.0
BINARY_NAME = grafts

# Install ocb if not present
install-ocb:
	@which builder > /dev/null || go install go.opentelemetry.io/collector/cmd/builder@v$(OCB_VERSION)

# Build the collector
build: install-ocb
	builder --config=manifest.yaml

# Run the collector with sample config
run: build
	./build/$(BINARY_NAME) --config=config.yaml

# Clean build artifacts
clean:
	rm -rf ./build

# Validate configuration
validate: build
	./build/$(BINARY_NAME) validate --config=config.yaml
