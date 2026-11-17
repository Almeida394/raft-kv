# raft-kv

A distributed key-value store in Go, with a Raft consensus implementation written from scratch (no `hashicorp/raft`).

> Work in progress — this README will be filled in as the project progresses (architecture diagram, benchmarks, trade-offs, limitations).

## Status

- [x] Project scaffold
- [ ] Write-ahead log + KV storage engine
- [ ] Leader election
- [ ] Log replication
- [ ] Snapshots & log compaction
- [ ] gRPC transport
- [ ] HTTP + gRPC client API
- [ ] Fault-injection tests
- [ ] Benchmarks
- [ ] Docker Compose cluster
