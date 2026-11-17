package transport

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/Almeida394/raft-kv/internal/raft"
	"github.com/Almeida394/raft-kv/internal/transport/raftpb"
)

// Serve starts a gRPC server exposing handler as the RaftTransport
// service on addr (e.g. "0.0.0.0:7000" or "127.0.0.1:0" to let the OS
// pick a free port). It returns immediately; the server runs on its own
// goroutine until the returned *grpc.Server is stopped.
func Serve(addr string, handler raft.RPCHandler) (srv *grpc.Server, actualAddr net.Addr, err error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("transport: listen on %s: %w", addr, err)
	}

	srv = grpc.NewServer()
	raftpb.RegisterRaftTransportServer(srv, NewServer(handler))

	go func() {
		_ = srv.Serve(lis) // returns once srv.Stop()/GracefulStop() is called
	}()

	return srv, lis.Addr(), nil
}
