package server

import (
	"fmt"
	"sync"
	"sync/atomic"
)

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
// StreamEntry
// ─────────────────────────────────────────────────────────────────────────────

type StreamEntry struct {
	Name   string
	ID     uint32
	Buffer *StreamBuffer
}

// ─────────────────────────────────────────────────────────────────────────────
// NodeRegistry
// ─────────────────────────────────────────────────────────────────────────────

type NodeRegistry struct {
	mu    sync.RWMutex
	nodes map[string]*EdgeSession
}

func NewNodeRegistry() *NodeRegistry {
	return &NodeRegistry{nodes: make(map[string]*EdgeSession)}
}

func (r *NodeRegistry) Register(nodeID string, sess *EdgeSession) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.nodes[nodeID]; ok && old != sess {
		old.closeWithReason("replaced by new connection")
	}
	r.nodes[nodeID] = sess
}

func (r *NodeRegistry) Unregister(nodeID string, sess *EdgeSession) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.nodes[nodeID]; ok && cur == sess {
		delete(r.nodes, nodeID)
	}
}

func (r *NodeRegistry) Get(nodeID string) *EdgeSession {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nodes[nodeID]
}

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
// SubscriptionTable
// ─────────────────────────────────────────────────────────────────────────────

type SubscriptionTable struct {
	mu   sync.RWMutex
	subs map[string]map[string]struct{}
}

func NewSubscriptionTable() *SubscriptionTable {
	return &SubscriptionTable{subs: make(map[string]map[string]struct{})}
}

func (t *SubscriptionTable) Subscribe(streamName, nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.subs[streamName] == nil {
		t.subs[streamName] = make(map[string]struct{})
	}
	t.subs[streamName][nodeID] = struct{}{}
}

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
// StreamRegistry — with O(1) GetByID via reverse index
// ─────────────────────────────────────────────────────────────────────────────

type StreamRegistry struct {
	mu        sync.RWMutex
	streams   map[string]*StreamEntry
	byID      map[uint32]*StreamEntry // FIX: reverse index for O(1) lookup
	maxRetain int64
}

func NewStreamRegistry(maxRetain int64) *StreamRegistry {
	return &StreamRegistry{
		streams:   make(map[string]*StreamEntry),
		byID:      make(map[uint32]*StreamEntry),
		maxRetain: maxRetain,
	}
}

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
	r.byID[e.ID] = e
	return e
}

func (r *StreamRegistry) Get(name string) *StreamEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.streams[name]
}

// GetByID returns the StreamEntry with the given uint32 stream ID.
// FIX: O(1) via reverse index instead of O(n) linear scan.
func (r *StreamRegistry) GetByID(id uint32) (*StreamEntry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e, ok := r.byID[id]; ok {
		return e, nil
	}
	return nil, fmt.Errorf("unknown stream id %d", id)
}

// Remove deletes a finished stream from the registry.
func (r *StreamRegistry) Remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.streams[name]; ok {
		delete(r.byID, e.ID)
		delete(r.streams, name)
	}
}

// ClosedStreams returns the names of all streams whose buffer has been closed.
func (r *StreamRegistry) ClosedStreams() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []string
	for name, e := range r.streams {
		if e.Buffer.IsClosed() {
			out = append(out, name)
		}
	}
	return out
}

func (r *StreamRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.streams))
	for n := range r.streams {
		out = append(out, n)
	}
	return out
}
