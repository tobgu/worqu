# Variables
PACKAGE=./...
BIN_NAME=your_binary_name
GOLANGCI_LINT_VERSION=v1.62.0
BUILD_TAG=test_tweaks

.PHONY: all build test test-verbose test-cover lint install-linter etcd-up ci

# Default target
all: build

# Target to build the library
build:
	go build -o $(BIN_NAME)

# Target to run all tests with verbose flag enabled
test:
	go test -tags $(BUILD_TAG) -count=1 -v $(PACKAGE)

# Target to run all tests with coverage enabled
test-cover:
	go test -tags $(BUILD_TAG) -count=1 -cover -coverprofile=coverage.out $(PACKAGE)
	go tool cover -html=coverage.out

# Target to run linting using golangci-lint
lint: install-linter
	./bin/golangci-lint run ./...

# Target to install golangci-lint if not already installed
install-linter:
	@if [ ! -e "./bin/golangci-lint" ] || [ "v$$(./bin/golangci-lint --version | head -n 1 | awk '{print $$4}')" != "$(GOLANGCI_LINT_VERSION)" ]; then \
		echo "Installing or updating golangci-lint to version $(GOLANGCI_LINT_VERSION)..."; \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s $(GOLANGCI_LINT_VERSION); \
	else \
		echo "golangci-lint $(GOLANGCI_LINT_VERSION) is already installed."; \
	fi

etcd-up:
	docker compose up -d --wait --wait-timeout 30

ci: etcd-up lint test