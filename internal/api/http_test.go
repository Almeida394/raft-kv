package api_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Almeida394/raft-kv/internal/api"
	"github.com/Almeida394/raft-kv/internal/raft"
	"github.com/Almeida394/raft-kv/internal/raft/rafttest"
	"github.com/Almeida394/raft-kv/internal/storage"
)

type testNode struct {
	id   raft.NodeID
	raft *raft.Raft
	http *httptest.Server
}

// newHTTPCluster wires n nodes together over the in-memory rafttest
// network (fast, deterministic) for Raft, each fronted by a real
// httptest.Server for the HTTP API, so forwarding between nodes goes
// over actual HTTP requests on loopback sockets.
func newHTTPCluster(t *testing.T, n int) []*testNode {
	t.Helper()

	ids := make([]raft.NodeID, n)
	for i := range ids {
		ids[i] = raft.NodeID(fmt.Sprintf("node-%d", i))
	}

	network := rafttest.NewNetwork()
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

	nodes := make([]*testNode, n)
	for i, id := range ids {
		var peers []raft.NodeID
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		node, err := raft.NewRaft(raft.Config{
			ID:                 id,
			Peers:              peers,
			Transport:          network.Endpoint(id),
			DataDir:            t.TempDir(),
			ApplyFn:            apiServers[i].ApplyFn,
			ElectionTimeoutMin: 50 * time.Millisecond,
			ElectionTimeoutMax: 100 * time.Millisecond,
			HeartbeatInterval:  15 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewRaft(%s): %v", id, err)
		}
		network.Register(id, node)
		apiServers[i].Raft = node
		nodes[i] = &testNode{id: id, raft: node, http: httpServers[i]}
	}

	for _, n := range nodes {
		n.raft.Start()
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.raft.Stop()
			n.http.Close()
		}
	})

	return nodes
}

func awaitAPILeader(t *testing.T, nodes []*testNode, timeout time.Duration) *testNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.raft.IsLeader() {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %s", timeout)
	return nil
}

func httpDo(t *testing.T, method, url, body string) (status int, respBody string) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return resp.StatusCode, string(data)
}

func TestHTTP_PutGetDeleteOnLeader(t *testing.T) {
	nodes := newHTTPCluster(t, 3)
	leader := awaitAPILeader(t, nodes, 2*time.Second)

	if status, _ := httpDo(t, http.MethodPut, leader.http.URL+"/kv/greeting", "hello"); status != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", status)
	}
	if status, body := httpDo(t, http.MethodGet, leader.http.URL+"/kv/greeting", ""); status != http.StatusOK || body != "hello" {
		t.Fatalf("GET = (%d, %q), want (200, \"hello\")", status, body)
	}
	if status, _ := httpDo(t, http.MethodDelete, leader.http.URL+"/kv/greeting", ""); status != http.StatusOK {
		t.Fatalf("DELETE status = %d, want 200", status)
	}
	if status, _ := httpDo(t, http.MethodGet, leader.http.URL+"/kv/greeting", ""); status != http.StatusNotFound {
		t.Fatalf("GET after DELETE status = %d, want 404", status)
	}
}

func TestHTTP_GetMissingKeyReturns404(t *testing.T) {
	nodes := newHTTPCluster(t, 3)
	leader := awaitAPILeader(t, nodes, 2*time.Second)

	status, _ := httpDo(t, http.MethodGet, leader.http.URL+"/kv/does-not-exist", "")
	if status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", status)
	}
}

func TestHTTP_FollowerForwardsWritesAndReadsToLeader(t *testing.T) {
	nodes := newHTTPCluster(t, 3)
	leader := awaitAPILeader(t, nodes, 2*time.Second)

	var follower *testNode
	for _, n := range nodes {
		if n.id != leader.id {
			follower = n
			break
		}
	}

	if status, _ := httpDo(t, http.MethodPut, follower.http.URL+"/kv/k", "v1"); status != http.StatusOK {
		t.Fatalf("PUT via follower status = %d, want 200", status)
	}

	// Read back through the SAME follower (also forwarded) and directly
	// on the leader - both must see the write.
	if status, body := httpDo(t, http.MethodGet, follower.http.URL+"/kv/k", ""); status != http.StatusOK || body != "v1" {
		t.Fatalf("GET via follower = (%d, %q), want (200, \"v1\")", status, body)
	}
	if status, body := httpDo(t, http.MethodGet, leader.http.URL+"/kv/k", ""); status != http.StatusOK || body != "v1" {
		t.Fatalf("GET via leader = (%d, %q), want (200, \"v1\")", status, body)
	}

	if status, _ := httpDo(t, http.MethodDelete, follower.http.URL+"/kv/k", ""); status != http.StatusOK {
		t.Fatalf("DELETE via follower status = %d, want 200", status)
	}
	if status, _ := httpDo(t, http.MethodGet, leader.http.URL+"/kv/k", ""); status != http.StatusNotFound {
		t.Fatalf("GET via leader after forwarded DELETE status = %d, want 404", status)
	}
}
