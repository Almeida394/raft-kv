// Command raftkv starts one node of a raft-kv cluster: it serves the
// Raft consensus RPCs and the client-facing KV API (HTTP + gRPC) for a
// single process, pointed at its peers via static configuration.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/Almeida394/raft-kv/internal/api"
	"github.com/Almeida394/raft-kv/internal/api/kvpb"
	"github.com/Almeida394/raft-kv/internal/raft"
	"github.com/Almeida394/raft-kv/internal/storage"
	"github.com/Almeida394/raft-kv/internal/transport"
	"github.com/Almeida394/raft-kv/internal/transport/raftpb"
)

// peerSpec is one entry of -peers: another node's id and the two
// addresses it listens on.
type peerSpec struct {
	id       raft.NodeID
	raftAddr string
	httpAddr string
}

func parsePeers(spec string) ([]peerSpec, error) {
	if spec == "" {
		return nil, nil
	}
	var peers []peerSpec
	for _, entry := range strings.Split(spec, ",") {
		parts := strings.Split(entry, "=")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid -peers entry %q: want id=raftAddr=httpAddr", entry)
		}
		peers = append(peers, peerSpec{
			id:       raft.NodeID(parts[0]),
			raftAddr: parts[1],
			httpAddr: parts[2],
		})
	}
	return peers, nil
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("raftkv: %v", err)
	}
}

func run() error {
	id := flag.String("id", "", "this node's unique id (required)")
	dataDir := flag.String("data-dir", "", "directory for this node's persistent state (required)")
	raftAddr := flag.String("raft-addr", "127.0.0.1:7000", "address this node listens on for Raft + gRPC KV traffic")
	httpAddr := flag.String("http-addr", "127.0.0.1:8000", "address this node listens on for the HTTP KV API")
	peersSpec := flag.String("peers", "", "other cluster members as id=raftAddr=httpAddr, comma-separated")
	snapshotThreshold := flag.Int("snapshot-threshold", 1000, "entries applied past the last snapshot before taking a new one (0 disables snapshotting)")
	electionTimeoutMin := flag.Duration("election-timeout-min", 150*time.Millisecond, "minimum randomized election timeout")
	electionTimeoutMax := flag.Duration("election-timeout-max", 300*time.Millisecond, "maximum randomized election timeout")
	heartbeatInterval := flag.Duration("heartbeat-interval", 50*time.Millisecond, "leader heartbeat interval")
	flag.Parse()

	if *id == "" {
		return errors.New("-id is required")
	}
	if *dataDir == "" {
		return errors.New("-data-dir is required")
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	peers, err := parsePeers(*peersSpec)
	if err != nil {
		return err
	}

	var peerIDs []raft.NodeID
	raftAddrs := make(map[raft.NodeID]string, len(peers))
	httpAddrs := make(map[raft.NodeID]string, len(peers))
	for _, p := range peers {
		peerIDs = append(peerIDs, p.id)
		raftAddrs[p.id] = p.raftAddr
		httpAddrs[p.id] = p.httpAddr
	}

	kv := storage.NewKVStore()
	raftClient := transport.NewClient(raftAddrs)
	apiServer := api.NewServer(kv, httpAddrs, raftClient.ConnFor)

	node, err := raft.NewRaft(raft.Config{
		ID:                 raft.NodeID(*id),
		Peers:              peerIDs,
		Transport:          raftClient,
		DataDir:            *dataDir,
		StateMachine:       kv,
		SnapshotThreshold:  *snapshotThreshold,
		ApplyFn:            apiServer.ApplyFn,
		ElectionTimeoutMin: *electionTimeoutMin,
		ElectionTimeoutMax: *electionTimeoutMax,
		HeartbeatInterval:  *heartbeatInterval,
		Logger:             log.New(os.Stderr, fmt.Sprintf("[%s] ", *id), log.LstdFlags),
	})
	if err != nil {
		return fmt.Errorf("create raft node: %w", err)
	}
	apiServer.Raft = node

	// One gRPC server hosts both the inter-node Raft transport and the
	// client-facing KV service, on the single -raft-addr port.
	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftTransportServer(grpcServer, transport.NewServer(node))
	kvpb.RegisterKVServer(grpcServer, api.NewGRPCServer(apiServer))

	lis, err := net.Listen("tcp", *raftAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *raftAddr, err)
	}
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("[%s] grpc server stopped: %v", *id, err)
		}
	}()

	httpServer := &http.Server{
		Addr:    *httpAddr,
		Handler: apiServer.HTTPHandler(),
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[%s] http server stopped: %v", *id, err)
		}
	}()

	node.Start()
	log.Printf("[%s] raft+grpc on %s, http on %s, %d peer(s)", *id, *raftAddr, *httpAddr, len(peers))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Printf("[%s] shutting down", *id)
	node.Stop()
	grpcServer.GracefulStop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(ctx)
	_ = raftClient.Close()
	return nil
}
