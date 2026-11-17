# raft-kv

A distributed key-value store in Go, with a Raft consensus implementation written from scratch (no `hashicorp/raft`).

> Work in progress — this README will be filled in as the project progresses (architecture diagram, benchmarks, trade-offs, limitations).

## Status

- [x] Project scaffold
- [x] Write-ahead log + KV storage engine
- [x] Leader election
- [x] Log replication
- [x] Snapshots & log compaction
- [x] gRPC transport
- [x] HTTP + gRPC client API
- [x] Fault-injection tests
- [x] Benchmarks
- [x] Docker Compose cluster
- [ ] Final README pass (architecture diagram, trade-offs, limitations)
