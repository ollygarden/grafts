.PHONY: test lint fmt tidy build test-integration telemetry-check telemetry-generate

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
	@echo "Testing receiver/pgbouncerreceiver..."
	@go test -v ./receiver/pgbouncerreceiver/...
	@echo "Testing internal/..."
	@go test -v ./internal/...
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
	@echo "Linting receiver/pgbouncerreceiver..."
	@golangci-lint run ./receiver/pgbouncerreceiver/...
	@echo "Linting internal/..."
	@golangci-lint run ./internal/...
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
	@go test -tags=integration -timeout=300s ./receiver/pgbouncerreceiver/...

# Validate every component's semantic-convention registry, then prove each Rego
# policy still fires. Requires Docker; the Weaver version and the upstream
# semconv pin live in telemetry/weaver.sh.
telemetry-check:
	@for r in receiver/*/telemetry/registry exporter/*/telemetry/registry; do \
		[ -d "$$r" ] || continue; \
		c="$$(dirname "$$(dirname "$$r")")"; \
		echo "Checking $$r..."; \
		./telemetry/weaver.sh check "$$r" || exit 1; \
		go run ./internal/registrycheck/cmd/stability-disclosure "$$r" "$$c/telemetry/component.yaml" || exit 1; \
	done
	@echo "Proving the policies fire..."
	@./telemetry/policies/run-fixtures.sh

# Regenerate the telemetry each component's registry defines. The output is
# committed and CI re-runs this to assert it is current, so a registry change
# that was never generated fails the build rather than shipping.
telemetry-generate:
	@for r in receiver/*/telemetry/registry exporter/*/telemetry/registry; do \
		[ -d "$$r" ] || continue; \
		c="$$(dirname "$$(dirname "$$r")")"; \
		echo "Generating $$c/internal/telemetry..."; \
		./telemetry/weaver.sh generate "$$r" "$$c/internal/telemetry" || exit 1; \
		mv "$$c/internal/telemetry/migration.md" "$$c/migration.md"; \
	done
