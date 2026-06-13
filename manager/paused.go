package manager

import (
	"sort"
	"sync"
)

// pausedSet is a concurrency-safe set of paused queue names.
//
// All read operations (contains, snapshot, len) acquire a read lock and can
// proceed concurrently. Write operations (add, remove, replaceFrom) acquire
// an exclusive write lock.
//
// The zero value is ready to use.
type pausedSet struct {
	mu   sync.RWMutex
	name map[string]struct{}
}

// newPausedSet returns a pausedSet pre-populated with the given names.
// Duplicates in the input are silently ignored.
func newPausedSet(names []string) *pausedSet {
	ps := &pausedSet{
		name: make(map[string]struct{}, len(names)),
	}
	for _, n := range names {
		ps.name[n] = struct{}{}
	}
	return ps
}

// add inserts name into the set. If name is already present this is a no-op.
func (ps *pausedSet) add(name string) {
	ps.mu.Lock()
	ps.name[name] = struct{}{}
	ps.mu.Unlock()
}

// remove deletes name from the set. If name is not present this is a no-op.
func (ps *pausedSet) remove(name string) {
	ps.mu.Lock()
	delete(ps.name, name)
	ps.mu.Unlock()
}

// contains reports whether name is in the set.
func (ps *pausedSet) contains(name string) bool {
	ps.mu.RLock()
	_, ok := ps.name[name]
	ps.mu.RUnlock()
	return ok
}

// len returns the number of elements in the set.
func (ps *pausedSet) len() int {
	ps.mu.RLock()
	n := len(ps.name)
	ps.mu.RUnlock()
	return n
}

// snapshot returns a sorted copy of the current set members.
// The returned slice is safe to use without further synchronization.
func (ps *pausedSet) snapshot() []string {
	ps.mu.RLock()
	out := make([]string, 0, len(ps.name))
	for k := range ps.name {
		out = append(out, k)
	}
	ps.mu.RUnlock()
	sort.Strings(out)
	return out
}

// filterActive returns a new slice containing only the elements of queues
// that are NOT in the paused set. The returned slice preserves the order of
// the input.
//
// This is the hot-path operation called from Fetch on every dequeue attempt,
// so it uses a read lock and O(1) map lookups instead of the previous O(n*m)
// linear scan over two slices.
func (ps *pausedSet) filterActive(queues []string) []string {
	if ps.len() == 0 {
		// Fast path: nothing paused, return queues as-is (no allocation).
		return queues
	}

	ps.mu.RLock()
	out := make([]string, 0, len(queues))
	for _, q := range queues {
		if _, paused := ps.name[q]; !paused {
			out = append(out, q)
		}
	}
	ps.mu.RUnlock()
	return out
}

// replaceFrom atomically replaces the set contents with the given names.
// Used during manager initialization to load state from Redis.
func (ps *pausedSet) replaceFrom(names []string) {
	m := make(map[string]struct{}, len(names))
	for _, n := range names {
		m[n] = struct{}{}
	}
	ps.mu.Lock()
	ps.name = m
	ps.mu.Unlock()
}
