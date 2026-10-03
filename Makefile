MODULE := github.com/LindseyZ1205/distributed-kv-store
PROTOS := proto/kv/v1/kv.proto proto/raft/v1/raft.proto
COMPOSE := docker compose -f deploy/docker-compose.yml
NODES := node1:7000,node2:7000,node3:7000

.PHONY: generate build test image cluster bench fault-test down

# Needs protoc, protoc-gen-go and protoc-gen-go-grpc on PATH.
generate:
	protoc -I proto \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		$(PROTOS)

build:
	go build -o bin/ ./cmd/...

test:
	go test -race ./...

image:
	docker build -f deploy/Dockerfile -t distributed-kv-store:local .

cluster: image
	$(COMPOSE) up -d --wait

bench:
	$(COMPOSE) run --rm --entrypoint /usr/local/bin/kvbench tools -addrs $(NODES)

fault-test:
	./scripts/fault-test.sh

down:
	$(COMPOSE) down -v
