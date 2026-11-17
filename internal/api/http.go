package api

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/Almeida394/raft-kv/internal/storage"
)

// writeTimeout bounds how long a write request waits for its entry to be
// committed before giving up and reporting a timeout to the client.
const writeTimeout = 5 * time.Second

// HTTPHandler returns the mux for the client-facing HTTP API:
//
//	GET    /kv/{key}  -> 200 with the value, or 404
//	PUT    /kv/{key}  -> body is the new value; 200 once committed
//	DELETE /kv/{key}  -> 200 once committed (regardless of prior existence)
func (s *Server) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	mux.HandleFunc("PUT /kv/{key}", s.handlePut)
	mux.HandleFunc("DELETE /kv/{key}", s.handleDelete)
	return mux
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if !s.Raft.IsLeader() {
		s.forwardHTTP(w, r)
		return
	}

	key := r.PathValue("key")
	value, ok := s.KV.Get([]byte(key))
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(value)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	if !s.Raft.IsLeader() {
		s.forwardHTTP(w, r)
		return
	}

	key := r.PathValue("key")
	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "reading request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	cmd := storage.Command{Op: storage.OpPut, Key: []byte(key), Value: value}
	s.proposeAndWait(w, r, cmd)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if !s.Raft.IsLeader() {
		s.forwardHTTP(w, r)
		return
	}

	key := r.PathValue("key")
	cmd := storage.Command{Op: storage.OpDelete, Key: []byte(key)}
	s.proposeAndWait(w, r, cmd)
}

func (s *Server) proposeAndWait(w http.ResponseWriter, r *http.Request, cmd storage.Command) {
	index, _, ok := s.Raft.Propose(cmd.Encode())
	if !ok {
		// Lost leadership between the IsLeader check and Propose.
		http.Error(w, "not leader", http.StatusServiceUnavailable)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), writeTimeout)
	defer cancel()
	if err := s.WaitForIndex(ctx, index); err != nil {
		http.Error(w, "timed out waiting for commit: "+err.Error(), http.StatusGatewayTimeout)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// forwardHTTP proxies the request to the current leader's HTTP address
// and relays its response back verbatim, so any node can be used as the
// entry point without the client needing its own retry/redirect logic.
func (s *Server) forwardHTTP(w http.ResponseWriter, r *http.Request) {
	addr, ok := s.leaderHTTPAddr()
	if !ok {
		http.Error(w, "no leader known", http.StatusServiceUnavailable)
		return
	}

	outReq, err := http.NewRequestWithContext(r.Context(), r.Method, "http://"+addr+r.URL.RequestURI(), r.Body)
	if err != nil {
		http.Error(w, "building forward request: "+err.Error(), http.StatusInternalServerError)
		return
	}
	outReq.ContentLength = r.ContentLength

	resp, err := http.DefaultClient.Do(outReq)
	if err != nil {
		http.Error(w, "forwarding to leader: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
