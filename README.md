# Distributed Raft Key-Value Store

A highly available, distributed key-value store built in Go, backed by a custom implementation of the [Raft Consensus Algorithm](https://raft.github.io/). This project demonstrates professional distributed systems concepts, including leader election, log replication, fault tolerance.

## Features

- **Raft Consensus**: Full implementation of the Raft protocol (Leader Election, Log Replication, Log Truncation/Snapshots).
- **Strong Consistency**: Ensures all operations are strictly serialized and consistent across the cluster.
- **Smart CLI Client**: An `etcd`-like command-line interface built with Cobra that automatically routes requests to the active leader.
- **Structured Observability**: Built-in JSON structured logging (`log/slog`)

## Architecture

The system consists of three main components:
1. **Raft Core (`internal/raft`)**: The consensus engine that manages the distributed state machine, elections, and peer heartbeats over gRPC.
2. **KV Store (`internal/kvstore`)**: The application layer that applies committed Raft logs to a thread-safe in-memory map.
3. **Smart Client (`cmd/client`)**: A Cobra-based CLI that interacts with the cluster, gracefully handling `ErrWrongLeader` responses by dynamically seeking the active leader.

## Getting Started

### Prerequisites
- [Docker](https://www.docker.com/) & Docker Compose
- [Go 1.21+](https://golang.org/) (for local builds)

### Running the Cluster

The easiest way to run the cluster is via Docker Compose. This spins up a 3-node Raft cluster.

```bash
# Build and start the cluster in the background
docker-compose up --build -d

# View the structured JSON logs
docker-compose logs -f
```

### Building Locally

```bash
# Build the server and client binaries
make build
# Or manually:
go build -o bin/kvserver ./cmd/kvserver
go build -o bin/client ./cmd/client
```

## Usage

Interact with the cluster using the built-in CLI client. By default, the client is configured to connect to the local Docker Compose cluster endpoints.

### Put a Value
```bash
go run ./cmd/client put mykey "Hello Distributed World"
```
*Notice how the client automatically routes to the leader if it hits a follower first!*

### Get a Value
```bash
go run ./cmd/client get mykey
# Output: SUCCESS! Found value: 'Hello Distributed World' (Version: 1)
```

## Project Structure

```text
.
├── cmd/
│   ├── client/       # Cobra CLI client
│   └── kvserver/     # Main server entry point
├── internal/
│   ├── kvstore/      # Key-Value application state machine
│   └── raft/         # Raft consensus engine (election, replication, snapshots)
├── proto/            # gRPC Protobuf definitions
├── storage/          # Disk persistence layer for Raft state
└── docker-compose.yml
```
