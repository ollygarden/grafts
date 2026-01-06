.PHONY: test lint fmt tidy build

# All component modules
MODULES := receiver/natsjetstreamreceiver

# Run tests for all components
test:
	@for mod in $(MODULES); do \
		echo "Testing $$mod..."; \
		(cd $$mod && go test -v ./...) || exit 1; \
	done

# Run linter for all components
lint:
	@for mod in $(MODULES); do \
		echo "Linting $$mod..."; \
		(cd $$mod && golangci-lint run ./...) || exit 1; \
	done

# Format all components
fmt:
	@for mod in $(MODULES); do \
		echo "Formatting $$mod..."; \
		(cd $$mod && go fmt ./...) || exit 1; \
	done

# Run go mod tidy for all components
tidy:
	@for mod in $(MODULES); do \
		echo "Tidying $$mod..."; \
		(cd $$mod && go mod tidy) || exit 1; \
	done

# Build the test distribution
build:
	$(MAKE) -C distributions/grafts build
