BINARY  := sonata
PKG     := ./cmd/sonata
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w \
	-X main.version=$(VERSION) \
	-X main.commit=$(COMMIT) \
	-X main.date=$(DATE)

# Cross-compilation is a hard requirement, so cgo stays off everywhere.
export CGO_ENABLED := 0

PLATFORMS := darwin/arm64 darwin/amd64 linux/amd64 linux/arm64

.PHONY: all
all: lint test build

.PHONY: build
build:
	go build -ldflags '$(LDFLAGS)' -o bin/$(BINARY) $(PKG)

.PHONY: build-all
build-all:
	@for p in $(PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; \
		echo "building $$os/$$arch"; \
		GOOS=$$os GOARCH=$$arch go build -ldflags '$(LDFLAGS)' \
			-o bin/$(BINARY)-$$os-$$arch $(PKG) || exit 1; \
	done

.PHONY: install
install:
	go install -ldflags '$(LDFLAGS)' $(PKG)

# Unit tests. -short skips anything that spawns a daemon or sleeps.
# -race is not optional here: concurrent CLI clients are the normal case.
.PHONY: test
test:
	go test -race -short ./...

# Full suite including integration tests, which spawn a real daemon
# against a temp socket and database.
.PHONY: test-integ
test-integ:
	go test -race -count=1 ./...

.PHONY: cover
cover:
	go test -race -short -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "wrote coverage.html"

.PHONY: lint
lint: vet
	@command -v golangci-lint >/dev/null 2>&1 \
		|| { echo "golangci-lint not installed: https://golangci-lint.run/welcome/install/"; exit 1; }
	golangci-lint run

.PHONY: vet
vet:
	go vet ./...

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: clean
clean:
	rm -rf bin coverage.out coverage.html
