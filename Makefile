APP_NAME:=datagen
BIN:=bin
GOBIN:=$(shell pwd)/$(BIN)

.env:
	cp .env-template .env

test-env-var: .env
	$(eval include .env)
	$(eval export)

test: test-env-var
	go test -timeout 5m -count 1 -cover ./...

lint:
	$(BIN)/golangci-lint run -c .golangci.yaml

golangci-lint:
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh \
		| sh -s -- -b $(BIN) v2.4.0

pglocal-dev-kill:
	docker rm -f pglocal 2>/dev/null || true

pglocal-dev-run: pglocal-dev-kill
	docker run --name pglocal -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_USER=postgres -p 5432:5432 -d postgres