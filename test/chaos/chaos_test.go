// Package chaos exercises the raft-kv system (Raft + the KV state
// machine + the HTTP API) under the kind of faults a real deployment
// hits: a leader dying mid-write, a network partition, and a slow node.
// Unlike internal/raft's own tests, which check Raft's internal state
// (log entries, commitIndex, term), these drive the system the way a
// client would - HTTP requests - and check externally observable
// behavior: does a committed write survive, does only the majority side
// keep accepting writes, does a slow node still end up consistent.
package chaos

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Almeida394/raft-kv/internal/api"
	"github.com/Almeida394/raft-kv/internal/raft"
	"github.com/Almeida394/raft-kv/internal/raft/rafttest"
	"github.com/Almeida394/raft-kv/internal/storage"

	"net/http/httptest"
)

type node struct {
	id   raft.NodeID
	raft *raft.Raft
	kv   *storage.KVStore
	http *httptest.Server
}

// newCluster builds an n-node raft-kv system: Raft nodes wired over an
// in-memory, fault-injectable network, each fronted by a real HTTP
// server for the client API - the same shape a deployed cluster has,
// minus the OS process boundary (killing a node here means Stop()ing
// it, which is as close as an in-process test can get to a killed
// process: no more heartbeats, no more votes, no more disk writes).
func newCluster(t *testing.T, n int) ([]*node, *rafttest.Network) {
	t.Helper()

	ids := make([]raft.NodeID, n)
	for i := range ids {
		ids[i] = raft.NodeID(fmt.Sprintf("node-%d", i))
	}

	network := rafttest.NewNetwork()
	apiServers := make([]*api.Server, n)
	kvs := make([]*storage.KVStore, n)
	httpServers := make([]*httptest.Server, n)

	for i := range ids {
		kv := storage.NewKVStore()
		kvs[i] = kv
		s := api.NewServer(kv, nil, nil)
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

	nodes := make([]*node, n)
	for i, id := range ids {
		var peers []raft.NodeID
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		r, err := raft.NewRaft(raft.Config{
			ID:                 id,
			Peers:              peers,
			Transport:          network.Endpoint(id),
			DataDir:            t.TempDir(),
			ApplyFn:            apiServers[i].ApplyFn,
			ElectionTimeoutMin: 100 * time.Millisecond,
			ElectionTimeoutMax: 200 * time.Millisecond,
			HeartbeatInterval:  30 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewRaft(%s): %v", id, err)
		}
		network.Register(id, r)
		apiServers[i].Raft = r
		nodes[i] = &node{id: id, raft: r, kv: kvs[i], http: httpServers[i]}
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

	return nodes, network
}

func awaitLeader(t *testing.T, nodes []*node, timeout time.Duration) *node {
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
	t.Fatalf("no leader elected among %d nodes within %s", len(nodes), timeout)
	return nil
}

func put(t *testing.T, n *node, key, value string, timeout time.Duration) (status int, err error) {
	t.Helper()
	client := &http.Client{Timeout: timeout}
	req, _ := http.NewRequest(http.MethodPut, n.http.URL+"/kv/"+key, strings.NewReader(value))
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func get(t *testing.T, n *node, key string, timeout time.Duration) (status int, body string, err error) {
	t.Helper()
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(n.http.URL + "/kv/" + key)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(data), nil
}

// TestKillLeaderMidWrite verifies the core Raft safety guarantee as seen
// from a client: once a write has been acknowledged (HTTP 200), it must
// never be lost, even if the leader that committed it is killed a moment
// later - and the cluster must elect a new leader and keep serving
// writes afterward.
func TestKillLeaderMidWrite(t *testing.T) {
	nodes, _ := newCluster(t, 5)
	leader := awaitLeader(t, nodes, 3*time.Second)

	committed := map[string]string{"a": "1", "b": "2", "c": "3"}
	for k, v := range committed {
		if status, err := put(t, leader, k, v, 2*time.Second); err != nil || status != http.StatusOK {
			t.Fatalf("PUT %s=%s before kill: status=%d err=%v", k, v, status, err)
		}
	}

	// Propose one more entry directly (bypassing the blocking HTTP path)
	// and kill the leader immediately after, before we know whether it
	// reached a majority - simulating the process dying mid-write. This
	// entry's fate is deliberately left unchecked: what must hold is
	// that everything already-committed above survives regardless.
	leader.raft.Propose(storage.Command{Op: storage.OpPut, Key: []byte("maybe"), Value: []byte("lost-or-not")}.Encode())
	leaderID := leader.id
	leader.raft.Stop()

	var remaining []*node
	for _, n := range nodes {
		if n.id != leaderID {
			remaining = append(remaining, n)
		}
	}

	newLeader := awaitLeader(t, remaining, 3*time.Second)
	if newLeader.id == leaderID {
		t.Fatalf("new leader should differ from the killed one")
	}

	for k, want := range committed {
		deadline := time.Now().Add(3 * time.Second)
		var status int
		var body string
		var err error
		for time.Now().Before(deadline) {
			status, body, err = get(t, newLeader, k, 2*time.Second)
			if err == nil && status == http.StatusOK && body == want {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil || status != http.StatusOK || body != want {
			t.Fatalf("GET %s after leader kill = (%d, %q, %v), want (200, %q, nil) - committed data was lost", k, status, body, err, want)
		}
	}

	// The cluster must still be alive: a fresh write through the new
	// leader must succeed.
	if status, err := put(t, newLeader, "after-recovery", "yes", 2*time.Second); err != nil || status != http.StatusOK {
		t.Fatalf("PUT after recovery: status=%d err=%v", status, err)
	}
}

// TestNetworkPartition_OnlyMajorityMakesProgress splits a 5-node cluster
// into a majority (3, including the original leader) and a minority (2),
// fully isolated from each other. The majority must keep accepting
// writes; the minority must not be able to make progress (no leader it
// can reach, so writes there never complete) until the partition heals,
// after which it must catch up to the majority's state.
func TestNetworkPartition_OnlyMajorityMakesProgress(t *testing.T) {
	nodes, network := newCluster(t, 5)
	leader := awaitLeader(t, nodes, 3*time.Second)

	var minority []*node
	var majority []*node
	majority = append(majority, leader)
	for _, n := range nodes {
		if n.id == leader.id {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, n)
		} else {
			majority = append(majority, n)
		}
	}
	for _, n := range minority {
		network.Partition(n.id)
	}
	// Give the minority side time to notice (its election timeout fires,
	// it becomes a candidate, and - per the fix above - clears its
	// stale leaderID) before we rely on it no longer being able to
	// forward anywhere.
	time.Sleep(400 * time.Millisecond)

	// Majority side keeps working.
	if status, err := put(t, leader, "during-partition", "majority-wrote-this", 2*time.Second); err != nil || status != http.StatusOK {
		t.Fatalf("PUT on majority side during partition: status=%d err=%v", status, err)
	}

	// Minority side can't: it has no leader to forward to (its only
	// peers are each other, also isolated), so a write there must fail
	// or time out rather than silently succeed.
	status, err := put(t, minority[0], "during-partition-minority", "should-not-land", 1*time.Second)
	if err == nil && status == http.StatusOK {
		t.Fatalf("PUT on minority side during partition unexpectedly succeeded")
	}

	for _, n := range minority {
		network.Heal(n.id)
	}

	for _, n := range nodes {
		deadline := time.Now().Add(3 * time.Second)
		var status int
		var body string
		for time.Now().Before(deadline) {
			status, body, _ = get(t, n, "during-partition", 2*time.Second)
			if status == http.StatusOK && body == "majority-wrote-this" {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if status != http.StatusOK || body != "majority-wrote-this" {
			t.Fatalf("node %s after healing: GET during-partition = (%d, %q), want (200, \"majority-wrote-this\")", n.id, status, body)
		}
	}
}

// TestSlowFollowerStillCatchesUp injects extra latency on one follower's
// outbound link (making its replies, and any votes it would cast, slow
// to arrive) and verifies it still ends up with the same state as
// everyone else - replication tolerates a slow node, it just takes
// longer.
func TestSlowFollowerStillCatchesUp(t *testing.T) {
	nodes, network := newCluster(t, 5)
	leader := awaitLeader(t, nodes, 3*time.Second)

	var slow *node
	for _, n := range nodes {
		if n.id != leader.id {
			slow = n
			break
		}
	}
	network.SetDelay(slow.id, 50*time.Millisecond) // well under the 100ms election timeout, well over the 30ms heartbeat interval

	want := map[string]string{"x": "1", "y": "2", "z": "3"}
	for k, v := range want {
		if status, err := put(t, leader, k, v, 2*time.Second); err != nil || status != http.StatusOK {
			t.Fatalf("PUT %s=%s: status=%d err=%v", k, v, status, err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && slow.kv.Len() < len(want) {
		time.Sleep(20 * time.Millisecond)
	}
	if slow.kv.Len() != len(want) {
		t.Fatalf("slow follower %s has %d keys after 5s, want %d", slow.id, slow.kv.Len(), len(want))
	}
	for k, v := range want {
		got, ok := slow.kv.Get([]byte(k))
		if !ok || string(got) != v {
			t.Fatalf("slow follower %s[%s] = (%q, %v), want (%q, true)", slow.id, k, got, ok, v)
		}
	}

	// It must also still be a healthy, voting member: if the leader
	// dies now, the cluster (slow node included) must still be able to
	// elect a new one.
	leaderID := leader.id
	leader.raft.Stop()
	var remaining []*node
	for _, n := range nodes {
		if n.id != leaderID {
			remaining = append(remaining, n)
		}
	}
	awaitLeader(t, remaining, 3*time.Second)
}
