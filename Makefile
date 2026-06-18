.PHONY: test lint fmt tidy build test-integration

# All component packages
PACKAGES := ./receiver/... ./exporter/...

# Run tests for all components
test:
	@echo "Testing receiver/natsjetstreamreceiver..."
	@go test -v ./receiver/natsjetstreamreceiver/...
	@echo "Testing exporter/natsjetstreamexporter..."
	@go test -v ./exporter/natsjetstreamexporter/...
	@echo "Testing receiver/snmpreceiver..."
	@go test -v ./receiver/snmpreceiver/...
	@echo "Testing exporter/parquetexporter..."
	@go test -v ./exporter/parquetexporter/...

# Run linter for all components
lint:
	@echo "Linting receiver/natsjetstreamreceiver..."
	@golangci-lint run ./receiver/natsjetstreamreceiver/...
	@echo "Linting exporter/natsjetstreamexporter..."
	@golangci-lint run ./exporter/natsjetstreamexporter/...
	@echo "Linting receiver/snmpreceiver..."
	@golangci-lint run ./receiver/snmpreceiver/...
	@echo "Linting exporter/parquetexporter..."
	@golangci-lint run ./exporter/parquetexporter/...

# Format all components
fmt:
	@go fmt $(PACKAGES)

# Run go mod tidy
tidy:
	@go mod tidy

# Build the test distribution
build:
	$(MAKE) -C distributions/grafts build

# Run integration tests (require Docker). Tests skip themselves if Docker is unavailable.
test-integration:
	@echo "Running integration tests..."
	@go test -tags=integration -timeout=300s ./receiver/snmpreceiver/...
