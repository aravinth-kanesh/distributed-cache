BINARY := dcache
SRC    := cmd/server/main.go

.PHONY: build test bench vet lint docker clean run

build:
	go build -o $(BINARY) $(SRC)

test:
	go test ./... -race -count=1

bench:
	go test ./test/benchmark/... -bench=. -benchmem

vet:
	go vet ./...

docker:
	docker compose up --build -d

docker-down:
	docker compose down -v

run: build
	./$(BINARY)

redis-bench: build
	./scripts/bench.sh

clean:
	rm -f $(BINARY)
	rm -rf data/
