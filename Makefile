APP_NAME:=datagen
BIN:=bin


lint:
	$(BIN)/golangci-lint run -c .golangci.yaml

golangci-lint:
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh \
		| sh -s -- -b $(BIN) v2.4.0