APP_NAME:=datagen
BIN:=bin
GOBIN:=$(shell pwd)/$(BIN)

.env:
	cp .env-template .env

test-env-var: .env
	$(eval include .env)
	$(eval export)

build:
	go build -o $(BIN)/datagen cmd/datagen/main.go

e2e: test-env-var build
	TEST_DATAGEN_CONNECTION_TYPE=postgresql go test -timeout 5m -count 1 -v -cover ./tests/...

test: test-env-var
	TEST_DATAGEN_CONNECTION_TYPE=postgresql go test -timeout 5m -count 1 -v -cover ./...

lint:
	$(BIN)/golangci-lint run -c .golangci.yaml

golangci-lint:
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh \
		| sh -s -- -b $(BIN) v2.4.0

pglocal-dev-kill:
	docker rm -f pglocal 2>/dev/null || true

pglocal-dev-run: pglocal-dev-kill
	docker run --name pglocal -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_USER=postgres -p 5432:5432 -d postgres