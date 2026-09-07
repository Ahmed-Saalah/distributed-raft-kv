.PHONY: build clean run-cluster

build:
	@echo "Building Raft KV Server..."
	@go build -o bin/kvserver ./cmd/kvserver
	@echo "Building Client..."
	@go build -o bin/client ./cmd/client
	@echo "Build complete."

clean:
	@echo "Cleaning up..."
	@rm -rf bin/
	@rm -rf data/
	@echo "Clean complete."