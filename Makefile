APP_NAME:=datagen
BIN:=bin
GOBIN:=$(shell pwd)/$(BIN)
UNAMEM:=$(shell uname -m)
UNAMES:=$(shell uname -s)
BUILD_SCRIPT:=./scripts/build.sh

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

build:
	${BUILD_SCRIPT} OS=${OS} \
		ARCH=${ARCH} \
		BUILDMODE="default" \
		GO_BUILD_PATH="cmd/datagen/main.go" \
		COPY_CONTEXT="." \
		BIN_DST=${BIN}/${APP_NAME}

build-plugin:
	${BUILD_SCRIPT} OS=${OS} \
		ARCH=${ARCH} \
		BUILDMODE="plugin" \
		GO_BUILD_PATH=${SO_PATH} \
		COPY_CONTEXT="." \
		BIN_DST=${DST}

e2e-plugins:
	$(MAKE) build-plugin SO_PATH="tests/e2e/plugins/json/json.go" DST="tests/e2e/plugins/json/json.so"

e2e: test-env-var build e2e-plugins
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
