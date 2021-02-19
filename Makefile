generated:
	go run ./tools/prebuildstep.go

test:
	go test ./...

lint:
	golangci-lint run

check: lint test

.PHONY: generated test check lint