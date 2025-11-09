APP_NAME:=datagen
BIN:=bin
GOBIN:=$(shell pwd)/$(BIN)
UNAMEM:=$(shell uname -m)
UNAMES:=$(shell uname -s)

ifeq ($(UNAMEM), x86_64)
	ARCH ?= amd64
else
	ARCH ?= arm64
endif


ifeq ($(UNAMES), Linux)
	OS ?= linux
else ifeq ($(UNAMES), Darwin)
	OS ?= darwin
else ifeq ($(findstring MINGW,$(UNAMES)), MINGW)
	OS ?= windows
else ifeq ($(findstring CYGWIN,$(UNAMES)), CYGWIN)
	OS ?= windows
else
	OS ?= unknown
endif

.env:
	cp .env-template .env

test-env-var: .env
	$(eval include .env)
	$(eval export)

docker-build:
	@echo "Building Docker image..."
	docker build -f docker/Dockerfile --build-arg OS=$(OS) --build-arg ARCH=$(ARCH) -t datagenimg .

	@echo "Extracting binary..."
	@id=$$(docker create datagenimg); \
	echo "Container ID: $$id"; \
	docker cp $$id:/app/datagen $(BIN)/datagen; \
	docker rm -v $$id

build:
	go build -o $(BIN)/datagen cmd/datagen/main.go

e2e: test-env-var build
	TEST_DATAGEN_CONNECTION_TYPE=postgresql go test -timeout 5m -count 1 -v -cover ./tests/...

test: test-env-var
	TEST_DATAGEN_CONNECTION_TYPE=postgresql go test -timeout 5m -count 1 -v -cover ./internal/... ./cmd/...

lint:
	$(BIN)/golangci-lint run -c .golangci.yaml

golangci-lint:
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh \
		| sh -s -- -b $(BIN) v2.4.0

pglocal-dev-kill:
	docker rm -f pglocal 2>/dev/null || true

pglocal-dev-run: pglocal-dev-kill
	docker run --name pglocal -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_USER=postgres -p 5432:5432 -d postgres