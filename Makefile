.PHONY: test lint fmt tidy build

# All component packages
PACKAGES := ./receiver/... ./exporter/...

# Run tests for all components
test:
	@echo "Testing receiver/natsjetstreamreceiver..."
	@go test -v ./receiver/natsjetstreamreceiver/...
	@echo "Testing exporter/natsjetstreamexporter..."
	@go test -v ./exporter/natsjetstreamexporter/...

# Run linter for all components
lint:
	@echo "Linting receiver/natsjetstreamreceiver..."
	@golangci-lint run ./receiver/natsjetstreamreceiver/...
	@echo "Linting exporter/natsjetstreamexporter..."
	@golangci-lint run ./exporter/natsjetstreamexporter/...

# Format all components
fmt:
	@go fmt $(PACKAGES)

# Run go mod tidy
tidy:
	@go mod tidy

# Build the test distribution
build:
	$(MAKE) -C distributions/grafts build
