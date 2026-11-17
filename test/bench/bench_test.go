// Package bench measures PUT throughput and latency (p50/p99) against a
// real raft-kv cluster: real gRPC connections between nodes (not the
// in-memory rafttest.Network internal/raft's own tests use) and a real
// HTTP server for the client API, all on loopback TCP - as close to the
// real deployed system as a single-machine benchmark can get.
//
// Run with a fixed operation count rather than Go's default time-based
// auto-calibration, since that would rebuild the cluster from scratch
// on every calibration pass:
//
//	go test -run=^$ -bench=. -benchtime=300x ./test/bench/...
//
// (the Makefile's `make bench` target does this already).
package bench

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/Almeida394/raft-kv/internal/api"
	"github.com/Almeida394/raft-kv/internal/raft"
	"github.com/Almeida394/raft-kv/internal/storage"
	"github.com/Almeida394/raft-kv/internal/transport"
	"github.com/Almeida394/raft-kv/internal/transport/raftpb"
)

type benchNode struct {
	id   raft.NodeID
	raft *raft.Raft
	grpc *grpc.Server
	http *httptest.Server
}

// newBenchCluster builds an n-node cluster wired exactly like
// cmd/raftkv: real gRPC between nodes (one server per node hosting the
// RaftTransport service, on a loopback port), a real HTTP server per
// node for the client API, and leader forwarding between them.
func newBenchCluster(tb testing.TB, n int, basePort int) []*benchNode {
	tb.Helper()

	ids := make([]raft.NodeID, n)
	raftAddrs := make(map[raft.NodeID]string, n)
	for i := range ids {
		ids[i] = raft.NodeID(fmt.Sprintf("node-%d", i))
		raftAddrs[ids[i]] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
	}

	nodes := make([]*benchNode, n)
	apiServers := make([]*api.Server, n)
	httpServers := make([]*httptest.Server, n)

	for i := range ids {
		s := api.NewServer(storage.NewKVStore(), nil, nil)
		apiServers[i] = s
		httpServers[i] = httptest.NewServer(s.HTTPHandler())
	}

	httpAddrs := make(map[raft.NodeID]string, n)
	for i, id := range ids {
		httpAddrs[id] = strings.TrimPrefix(httpServers[i].URL, "http://")
	}
	for _, s := range apiServers {
		s.HTTPAddrs = httpAddrs
	}

	for i, id := range ids {
		var peers []raft.NodeID
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		client := transport.NewClient(raftAddrs)
		r, err := raft.NewRaft(raft.Config{
			ID:                 id,
			Peers:              peers,
			Transport:          client,
			DataDir:            tb.TempDir(),
			ApplyFn:            apiServers[i].ApplyFn,
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
		})
		if err != nil {
			tb.Fatalf("NewRaft(%s): %v", id, err)
		}
		apiServers[i].Raft = r
		apiServers[i].PeerConn = client.ConnFor

		lis, err := net.Listen("tcp", raftAddrs[id])
		if err != nil {
			tb.Fatalf("listen %s: %v", raftAddrs[id], err)
		}
		grpcSrv := grpc.NewServer()
		raftpb.RegisterRaftTransportServer(grpcSrv, transport.NewServer(r))
		go func() { _ = grpcSrv.Serve(lis) }()

		nodes[i] = &benchNode{id: id, raft: r, grpc: grpcSrv, http: httpServers[i]}
	}

	for _, n := range nodes {
		n.raft.Start()
	}
	tb.Cleanup(func() {
		for _, n := range nodes {
			n.raft.Stop()
			n.grpc.Stop()
			n.http.Close()
		}
	})

	return nodes
}

func awaitLeader(tb testing.TB, nodes []*benchNode, timeout time.Duration) *benchNode {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.raft.IsLeader() {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	tb.Fatalf("no leader elected among %d nodes within %s", len(nodes), timeout)
	return nil
}

// runPutBenchmark drives `concurrency` goroutines issuing PUTs against
// the leader until a total of b.N operations have completed, recording
// every operation's latency, then reports throughput (ops/s) and p50/p99
// latency (µs) as benchmark metrics alongside the standard ns/op.
func runPutBenchmark(b *testing.B, nodeCount, concurrency, basePort int) {
	nodes := newBenchCluster(b, nodeCount, basePort)
	leader := awaitLeader(b, nodes, 5*time.Second)
	client := &http.Client{Timeout: 5 * time.Second}

	var mu sync.Mutex
	latencies := make([]time.Duration, 0, b.N)

	opCh := make(chan int, b.N)
	for i := 0; i < b.N; i++ {
		opCh <- i
	}
	close(opCh)

	value := strings.Repeat("v", 64) // representative small-value payload

	b.ResetTimer()
	start := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := range opCh {
				key := fmt.Sprintf("k-%d-%d", worker, i)
				t0 := time.Now()
				req, _ := http.NewRequest(http.MethodPut, leader.http.URL+"/kv/"+key, strings.NewReader(value))
				resp, err := client.Do(req)
				lat := time.Since(t0)
				if err != nil {
					b.Errorf("PUT: %v", err)
					return
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					b.Errorf("PUT status = %d, want 200", resp.StatusCode)
					return
				}
				mu.Lock()
				latencies = append(latencies, lat)
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()

	if len(latencies) == 0 {
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[len(latencies)*50/100]
	p99idx := len(latencies) * 99 / 100
	if p99idx >= len(latencies) {
		p99idx = len(latencies) - 1
	}
	p99 := latencies[p99idx]

	b.ReportMetric(float64(len(latencies))/elapsed.Seconds(), "ops/s")
	b.ReportMetric(float64(p50.Microseconds()), "p50-us")
	b.ReportMetric(float64(p99.Microseconds()), "p99-us")
}

func BenchmarkPut_3Node_Concurrency8(b *testing.B) {
	runPutBenchmark(b, 3, 8, 18100)
}

func BenchmarkPut_5Node_Concurrency8(b *testing.B) {
	runPutBenchmark(b, 5, 8, 18200)
}
