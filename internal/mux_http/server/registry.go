package server

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// nextStreamID is a monotonically increasing counter for allocating stream IDs.
// Stream ID 0 is reserved for control frames.
var nextStreamID atomic.Uint32

func init() { nextStreamID.Store(1) }

func allocStreamID() uint32 {
	for {
		id := nextStreamID.Add(1)
		if id != 0 {
			return id
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// StreamEntry — one named push stream managed by the server
// ─────────────────────────────────────────────────────────────────────────────

// StreamEntry holds the server-side state for one named push stream.
type StreamEntry struct {
	Name   string
	ID     uint32
	Buffer *StreamBuffer
}

// ─────────────────────────────────────────────────────────────────────────────
// NodeRegistry — maps nodeID → *EdgeSession
// ─────────────────────────────────────────────────────────────────────────────

// NodeRegistry keeps track of which edge nodes are currently connected.
type NodeRegistry struct {
	mu      sync.RWMutex
	nodes   map[string]*EdgeSession
}

func NewNodeRegistry() *NodeRegistry {
	return &NodeRegistry{nodes: make(map[string]*EdgeSession)}
}

// Register adds or replaces the session for nodeID.
// If an existing session is present it is closed before replacement.
func (r *NodeRegistry) Register(nodeID string, sess *EdgeSession) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.nodes[nodeID]; ok && old != sess {
		old.closeWithReason("replaced by new connection")
	}
	r.nodes[nodeID] = sess
}

// Unregister removes the session for nodeID if it matches sess.
func (r *NodeRegistry) Unregister(nodeID string, sess *EdgeSession) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.nodes[nodeID]; ok && cur == sess {
		delete(r.nodes, nodeID)
	}
}

// Get returns the active session for nodeID, or nil.
func (r *NodeRegistry) Get(nodeID string) *EdgeSession {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nodes[nodeID]
}

// All returns a snapshot of all active sessions.
func (r *NodeRegistry) All() []*EdgeSession {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*EdgeSession, 0, len(r.nodes))
	for _, s := range r.nodes {
		out = append(out, s)
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// SubscriptionTable — maps stream name → []nodeID
// ─────────────────────────────────────────────────────────────────────────────

// SubscriptionTable records which edge nodes are subscribed to each stream.
type SubscriptionTable struct {
	mu   sync.RWMutex
	subs map[string]map[string]struct{} // stream name → set of nodeIDs
}

func NewSubscriptionTable() *SubscriptionTable {
	return &SubscriptionTable{subs: make(map[string]map[string]struct{})}
}

// Subscribe records that nodeID wants to receive pushes for streamName.
func (t *SubscriptionTable) Subscribe(streamName, nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.subs[streamName] == nil {
		t.subs[streamName] = make(map[string]struct{})
	}
	t.subs[streamName][nodeID] = struct{}{}
}

// Unsubscribe removes nodeID from all streams.
func (t *SubscriptionTable) Unsubscribe(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for name, set := range t.subs {
		delete(set, nodeID)
		if len(set) == 0 {
			delete(t.subs, name)
		}
	}
}

// Subscribers returns the current set of nodeIDs subscribed to streamName.
func (t *SubscriptionTable) Subscribers(streamName string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	set, ok := t.subs[streamName]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// StreamRegistry — maps stream name → *StreamEntry
// ─────────────────────────────────────────────────────────────────────────────

// StreamRegistry holds all known push streams (both active and finished).
type StreamRegistry struct {
	mu      sync.RWMutex
	streams map[string]*StreamEntry
	// maxRetain controls StreamBuffer size; configurable per deployment.
	maxRetain int64
}

func NewStreamRegistry(maxRetain int64) *StreamRegistry {
	return &StreamRegistry{
		streams:   make(map[string]*StreamEntry),
		maxRetain: maxRetain,
	}
}

// GetOrCreate returns the existing StreamEntry for name, creating one if absent.
func (r *StreamRegistry) GetOrCreate(name string) *StreamEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.streams[name]; ok {
		return e
	}
	e := &StreamEntry{
		Name:   name,
		ID:     allocStreamID(),
		Buffer: NewStreamBuffer(r.maxRetain),
	}
	r.streams[name] = e
	return e
}

// Get returns the StreamEntry for name, or nil.
func (r *StreamRegistry) Get(name string) *StreamEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.streams[name]
}

// GetByID returns the StreamEntry with the given uint32 stream ID, or an error.
func (r *StreamRegistry) GetByID(id uint32) (*StreamEntry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, e := range r.streams {
		if e.ID == id {
			return e, nil
		}
	}
	return nil, fmt.Errorf("unknown stream id %d", id)
}

// Names returns a snapshot of all stream names.
func (r *StreamRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.streams))
	for n := range r.streams {
		out = append(out, n)
	}
	return out
}
