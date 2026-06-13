package server

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClientData(t *testing.T) {
	t.Parallel()

	cw, err := clientDataFromHello("")
	assert.Error(t, err)
	assert.Nil(t, cw)

	cw, err = clientDataFromHello("{")
	assert.Error(t, err)
	assert.Nil(t, cw)

	cw, err = clientDataFromHello("{}")
	assert.NoError(t, err)
	assert.NotNil(t, cw)
	assert.False(t, cw.IsConsumer())

	ahoy := `{"hostname":"MikeBookPro.local","wid":"78629a0f5f3f164f","pid":40275,"labels":["blue","seven"],"salt":"123456","pwdhash":"958d51602bbfbd18b2a084ba848a827c29952bfef170c936419b0922994c0589"}`
	cw, err = clientDataFromHello(ahoy)
	assert.NoError(t, err)
	assert.NotNil(t, cw)
	assert.True(t, cw.IsConsumer())

	assert.Equal(t, Running, cw.state)
	assert.False(t, cw.IsQuiet())

	cw.Signal(Quiet)
	assert.Equal(t, Quiet, cw.state)
	assert.True(t, cw.IsQuiet())

	cw.Signal(Terminate)
	assert.Equal(t, Terminate, cw.state)
	assert.True(t, cw.IsQuiet())

	// can't go back to quiet
	cw.Signal(Quiet)
	assert.Equal(t, Terminate, cw.state)
	assert.True(t, cw.IsQuiet())
}

func TestWorkers(t *testing.T) {
	t.Parallel()

	workers := newWorkers()
	assert.Equal(t, 0, workers.Count())

	beat := &ClientBeat{
		Wid: "78629a0f5f3f164f",
	}
	entry, ok := workers.heartbeat(beat)
	assert.Equal(t, 0, workers.Count())
	assert.Nil(t, entry)
	assert.False(t, ok)

	client := &ClientData{
		Hostname:    "MikeBookPro.local",
		Wid:         "78629a0f5f3f164f",
		connections: map[io.Closer]bool{},
	}
	entry, ok = workers.setupHeartbeat(client, &cls{})
	assert.NotNil(t, entry)
	assert.False(t, ok)

	entry, ok = workers.heartbeat(beat)
	assert.Equal(t, 1, workers.Count())
	assert.NotNil(t, entry)
	assert.True(t, ok)

	before := time.Now()
	entry, ok = workers.heartbeat(beat)
	after := time.Now()
	assert.Equal(t, 1, workers.Count())
	assert.NotNil(t, entry)
	assert.True(t, ok)
	assert.LessOrEqual(t, before, entry.lastHeartbeat)
	assert.LessOrEqual(t, entry.lastHeartbeat, after)

	assert.Equal(t, Running, entry.state)
	beat.CurrentState = "quiet"
	entry, _ = workers.heartbeat(beat)
	assert.Equal(t, Quiet, entry.state)
	assert.True(t, entry.IsQuiet())

	beat.CurrentState = ""
	entry, _ = workers.heartbeat(beat)
	assert.Equal(t, Quiet, entry.state)

	beat.CurrentState = "terminate"
	entry, _ = workers.heartbeat(beat)
	assert.Equal(t, Terminate, entry.state)

	count := workers.reapHeartbeats(client.lastHeartbeat)
	assert.Equal(t, 1, workers.Count())
	assert.Equal(t, 0, count)

	count = workers.reapHeartbeats(time.Now())
	assert.Equal(t, 0, workers.Count())
	assert.Equal(t, 1, count)
}

type cls struct{}

func (c cls) Close() error {
	return nil
}

func TestSnapshotReturnsCopy(t *testing.T) {
	t.Parallel()

	w := newWorkers()
	cd := &ClientData{
		Wid:         "wid-1",
		Hostname:    "host",
		Pid:         1,
		connections: map[io.Closer]bool{},
	}
	w.setupHeartbeat(cd, &cls{})

	snap := w.Snapshot()
	assert.Equal(t, 1, len(snap))
	assert.NotNil(t, snap["wid-1"])

	// Deleting from the snapshot must NOT affect the live map.
	delete(snap, "wid-1")
	assert.Equal(t, 1, w.Count(), "snapshot mutation should not affect live map")

	// The live map should still be intact.
	snap2 := w.Snapshot()
	assert.Equal(t, 1, len(snap2))
}

func TestSignalWorkers(t *testing.T) {
	t.Parallel()

	w := newWorkers()
	for _, wid := range []string{"a", "b", "c"} {
		cd := &ClientData{
			Wid:         wid,
			connections: map[io.Closer]bool{},
		}
		w.setupHeartbeat(cd, &cls{})
	}
	assert.Equal(t, 3, w.Count())

	// Signal a single worker
	w.SignalWorkers("a", Quiet)
	snap := w.Snapshot()
	assert.Equal(t, Quiet, snap["a"].state)
	assert.Equal(t, Running, snap["b"].state)
	assert.Equal(t, Running, snap["c"].state)

	// Signal all
	w.SignalWorkers("all", Quiet)
	snap = w.Snapshot()
	assert.Equal(t, Quiet, snap["a"].state)
	assert.Equal(t, Quiet, snap["b"].state)
	assert.Equal(t, Quiet, snap["c"].state)
}

func TestSignalWorkersIdempotent(t *testing.T) {
	t.Parallel()

	w := newWorkers()
	cd := &ClientData{
		Wid:         "x",
		connections: map[io.Closer]bool{},
	}
	w.setupHeartbeat(cd, &cls{})

	// Signalling a non-existent wid is a no-op
	w.SignalWorkers("nonexistent", Quiet)
	snap := w.Snapshot()
	assert.Equal(t, Running, snap["x"].state)
}

// TestConcurrentSnapshotAndMutations runs concurrent producers (heartbeat,
// setupHeartbeat, reapHeartbeats) and consumers (Snapshot, SignalWorkers)
// to verify that no data race occurs.  Run with -race to detect issues.
func TestConcurrentSnapshotAndMutations(t *testing.T) {
	t.Parallel()

	w := newWorkers()
	const workers = 20
	const iterations = 200

	// Pre-populate workers
	closers := make([]*cls, workers)
	for i := 0; i < workers; i++ {
		closers[i] = &cls{}
		cd := &ClientData{
			Wid:         widFor(i),
			Hostname:    "host",
			Pid:         i,
			connections: map[io.Closer]bool{},
		}
		w.setupHeartbeat(cd, closers[i])
	}

	var wg sync.WaitGroup

	// Goroutine 1: continuously send heartbeats
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			beat := &ClientBeat{Wid: widFor(i % workers)}
			w.heartbeat(beat)
		}
	}()

	// Goroutine 2: continuously take snapshots
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			snap := w.Snapshot()
			// Iterate the snapshot (simulating webui busyWorkers)
			for _, cd := range snap {
				_ = cd.Wid
				_ = cd.IsQuiet()
				_ = cd.ConnectionCount()
			}
		}
	}()

	// Goroutine 3: continuously signal workers (simulating busyHandler POST)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			w.SignalWorkers(widFor(i%workers), Quiet)
		}
	}()

	// Goroutine 4: continuously add connections (exercises setupHeartbeat lock path)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			idx := i % workers
			c := &cls{}
			// Re-register heartbeat with a new closer; setupHeartbeat will
			// add to the existing entry's connections map under the write lock.
			cd := &ClientData{
				Wid:         widFor(idx),
				Hostname:    "host",
				Pid:         idx,
				connections: map[io.Closer]bool{},
			}
			entry, _ := w.setupHeartbeat(cd, c)
			// Now build a Connection that wraps the *actual* entry so that
			// RemoveConnection can find and delete it from connections.
			conn := &Connection{client: entry, conn: &nopWriteCloser{}}
			// Register conn in the connections map so RemoveConnection finds it.
			w.mu.Lock()
			entry.connections[conn] = true
			w.mu.Unlock()
			w.RemoveConnection(conn)
		}
	}()

	// Goroutine 5: continuously reap heartbeats
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			// Use a time in the past so nothing gets reaped during the race test
			w.reapHeartbeats(time.Now().Add(-1 * time.Hour))
		}
	}()

	wg.Wait()
}

// TestConcurrentSnapshotReadAfterReap verifies that a snapshot taken before
// a reap is still safely iterable even after the underlying entries are removed.
func TestConcurrentSnapshotReadAfterReap(t *testing.T) {
	t.Parallel()

	w := newWorkers()
	cd := &ClientData{
		Wid:           "reapme",
		connections:   map[io.Closer]bool{},
		lastHeartbeat: time.Now().Add(-10 * time.Minute),
	}
	w.setupHeartbeat(cd, &cls{})

	// Take a snapshot before reaping
	snap := w.Snapshot()
	assert.Equal(t, 1, len(snap))

	// Reap the worker (its lastHeartbeat is in the past)
	reaped := w.reapHeartbeats(time.Now())
	assert.Equal(t, 1, reaped)
	assert.Equal(t, 0, w.Count())

	// The snapshot should still be safely iterable (the pointer is still valid).
	for _, v := range snap {
		assert.Equal(t, "reapme", v.Wid)
	}
}

func widFor(i int) string {
	return "wid-" + string(rune('A'+i))
}

// nopWriteCloser satisfies io.WriteCloser for test Connection objects.
type nopWriteCloser struct{}

func (nopWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (nopWriteCloser) Close() error                 { return nil }
