GO ?= go

.PHONY: build test vet fmt fmt-check check demo

build:
	mkdir -p bin
	$(GO) build -trimpath -o bin/gke-drain ./cmd/gke-drain

test:
	$(GO) test -race -cover ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -w .

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

check: fmt-check vet test

demo:
	$(GO) run ./cmd/gke-drain --demo --target-per-zone=2
