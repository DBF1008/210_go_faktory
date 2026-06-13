package server

import (
	"fmt"
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

// cls is a no-op io.Closer test double. It carries an id so that distinct
// instances are distinct map keys; an empty struct{} would be zero-sized and
// every &cls{} could share one address, collapsing into a single connection.
type cls struct{ id int }

func (c *cls) Close() error {
	return nil
}

func TestWorkersBusyStateSnapshot(t *testing.T) {
	t.Parallel()

	w := newWorkers()
	cd := &ClientData{Hostname: "h1", Wid: "wid-1", Labels: []string{"a"}}
	_, _ = w.setupHeartbeat(cd, &cls{id: 1})

	state := w.BusyState()
	assert.Len(t, state, 1)
	snap := state[0]
	assert.Equal(t, "wid-1", snap.Wid)
	assert.Equal(t, 1, snap.ConnectionCount())
	assert.Equal(t, Running, snap.state)

	// The snapshot must be an independent copy: signaling the live worker and
	// adding a connection after the snapshot was taken must NOT be observable
	// through the previously returned snapshot.
	assert.Equal(t, 1, w.SignalState("wid-1", Quiet))
	_, _ = w.setupHeartbeat(cd, &cls{id: 2}) // a second, distinct live connection

	assert.Equal(t, Running, snap.state, "snapshot must not observe a later signal")
	assert.False(t, snap.IsQuiet())
	assert.Equal(t, 1, snap.ConnectionCount(), "snapshot connection count must be stable")

	// A fresh snapshot reflects the new live state.
	state2 := w.BusyState()
	assert.Len(t, state2, 1)
	assert.Equal(t, Quiet, state2[0].state)
	assert.Equal(t, 2, state2[0].ConnectionCount())

	// Mutating a returned snapshot must not corrupt the live record.
	snap.state = Terminate
	assert.Equal(t, Quiet, w.heartbeats["wid-1"].state)
}

func TestWorkersSignalState(t *testing.T) {
	t.Parallel()

	w := newWorkers()
	_, _ = w.setupHeartbeat(&ClientData{Wid: "a"}, &cls{})
	_, _ = w.setupHeartbeat(&ClientData{Wid: "b"}, &cls{})

	assert.Equal(t, 1, w.SignalState("a", Quiet))
	assert.Equal(t, Quiet, w.heartbeats["a"].state)
	assert.Equal(t, Running, w.heartbeats["b"].state)

	assert.Equal(t, 2, w.SignalState("all", Terminate))
	assert.Equal(t, Terminate, w.heartbeats["a"].state)
	assert.Equal(t, Terminate, w.heartbeats["b"].state)

	assert.Equal(t, 0, w.SignalState("missing", Quiet))
}

// TestWorkersConcurrentSnapshot is a regression test for the data race that
// occurred when the Busy page iterated the live heartbeats map while workers
// were beating, registering, signaling, and being reaped. It must be run with
// -race to be meaningful.
func TestWorkersConcurrentSnapshot(t *testing.T) {
	t.Parallel()

	w := newWorkers()
	for i := 0; i < 8; i++ {
		_, _ = w.setupHeartbeat(&ClientData{Wid: fmt.Sprintf("wid-%d", i)}, &cls{})
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Readers continuously take snapshots and read fields off them, exactly as
	// the Busy page does via BusyState.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for _, s := range w.BusyState() {
						_ = s.Wid
						_ = s.ConnectionCount()
						_ = s.IsQuiet()
						_ = s.RssKb
					}
				}
			}
		}()
	}

	// Writers beat, signal, register new workers, and reap concurrently.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			wid := fmt.Sprintf("wid-%d", i)
			for {
				select {
				case <-stop:
					return
				default:
					w.heartbeat(&ClientBeat{Wid: wid, RssKb: int64(i)})
					w.SignalState(wid, Quiet)
					_, _ = w.setupHeartbeat(&ClientData{Wid: fmt.Sprintf("tmp-%d", i)}, &cls{})
					w.reapHeartbeats(time.Now().Add(time.Hour))
				}
			}
		}(i)
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	assert.GreaterOrEqual(t, w.Count(), 0)
}
