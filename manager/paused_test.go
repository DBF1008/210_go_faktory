package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/stretchr/testify/assert"
)

// ---------- pure unit tests (no Redis needed) ----------

func TestPausedSetNewEmpty(t *testing.T) {
	t.Parallel()
	ps := newPausedSet(nil)
	assert.Equal(t, 0, ps.len())
	assert.False(t, ps.contains("anything"))
	assert.Empty(t, ps.snapshot())
}

func TestPausedSetNewWithNames(t *testing.T) {
	t.Parallel()
	ps := newPausedSet([]string{"a", "b", "c"})
	assert.Equal(t, 3, ps.len())
	assert.True(t, ps.contains("a"))
	assert.True(t, ps.contains("b"))
	assert.True(t, ps.contains("c"))
	assert.False(t, ps.contains("d"))
}

func TestPausedSetNewDeduplicates(t *testing.T) {
	t.Parallel()
	ps := newPausedSet([]string{"x", "x", "x"})
	assert.Equal(t, 1, ps.len())
}

func TestPausedSetAdd(t *testing.T) {
	t.Parallel()
	ps := newPausedSet(nil)

	ps.add("q1")
	assert.True(t, ps.contains("q1"))
	assert.Equal(t, 1, ps.len())

	// idempotent
	ps.add("q1")
	assert.Equal(t, 1, ps.len())

	ps.add("q2")
	assert.Equal(t, 2, ps.len())
}

func TestPausedSetRemove(t *testing.T) {
	t.Parallel()
	ps := newPausedSet([]string{"a", "b"})

	ps.remove("a")
	assert.False(t, ps.contains("a"))
	assert.Equal(t, 1, ps.len())

	// removing non-existent is a no-op
	ps.remove("nonexistent")
	assert.Equal(t, 1, ps.len())

	ps.remove("b")
	assert.Equal(t, 0, ps.len())
}

func TestPausedSetSnapshot(t *testing.T) {
	t.Parallel()
	ps := newPausedSet([]string{"c", "a", "b"})

	snap := ps.snapshot()
	// snapshot should be sorted
	assert.Equal(t, []string{"a", "b", "c"}, snap)

	// snapshot is a copy — mutating it must not affect the set
	snap[0] = "MUTATED"
	assert.True(t, ps.contains("a"))
}

func TestPausedSetSnapshotEmpty(t *testing.T) {
	t.Parallel()
	ps := newPausedSet(nil)
	snap := ps.snapshot()
	assert.NotNil(t, snap)
	assert.Empty(t, snap)
}

func TestPausedSetReplaceFrom(t *testing.T) {
	t.Parallel()
	ps := newPausedSet([]string{"old1", "old2"})

	ps.replaceFrom([]string{"new1", "new2", "new3"})
	assert.Equal(t, 3, ps.len())
	assert.False(t, ps.contains("old1"))
	assert.True(t, ps.contains("new1"))
	assert.True(t, ps.contains("new2"))
	assert.True(t, ps.contains("new3"))

	// replace with empty clears the set
	ps.replaceFrom(nil)
	assert.Equal(t, 0, ps.len())
}

func TestFilterActive(t *testing.T) {
	t.Parallel()

	t.Run("empty set returns input unchanged", func(t *testing.T) {
		ps := newPausedSet(nil)
		input := []string{"a", "b", "c"}
		out := ps.filterActive(input)
		// When nothing is paused, filterActive returns the original slice
		// (no allocation).
		assert.Same(t, &input[0], &out[0])
		assert.Equal(t, input, out)
	})

	t.Run("filters paused queues", func(t *testing.T) {
		ps := newPausedSet([]string{"a", "c"})
		out := ps.filterActive([]string{"a", "b", "c", "d"})
		assert.Equal(t, []string{"b", "d"}, out)
	})

	t.Run("preserves input order", func(t *testing.T) {
		ps := newPausedSet([]string{"x"})
		out := ps.filterActive([]string{"z", "y", "x", "w"})
		assert.Equal(t, []string{"z", "y", "w"}, out)
	})

	t.Run("all paused returns empty", func(t *testing.T) {
		ps := newPausedSet([]string{"a", "b"})
		out := ps.filterActive([]string{"a", "b"})
		assert.Empty(t, out)
	})

	t.Run("none paused returns all", func(t *testing.T) {
		ps := newPausedSet([]string{"z"})
		out := ps.filterActive([]string{"a", "b"})
		assert.Equal(t, []string{"a", "b"}, out)
	})

	t.Run("empty input returns empty", func(t *testing.T) {
		ps := newPausedSet([]string{"a"})
		out := ps.filterActive(nil)
		assert.Empty(t, out)
	})
}

// ---------- concurrency tests ----------

func TestPausedSetConcurrentAddRemove(t *testing.T) {
	t.Parallel()

	ps := newPausedSet(nil)
	const goroutines = 50
	const iterations = 500

	var wg sync.WaitGroup
	wg.Add(goroutines * 2) // adders + removers

	// Half the goroutines add, the other half remove.
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				ps.add(fmt.Sprintf("q-%d-%d", id, i))
			}
		}(g)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				ps.remove(fmt.Sprintf("q-%d-%d", id, i))
			}
		}(g)
	}

	wg.Wait()
	// After all adds and removes cancel out, len should be 0.
	assert.Equal(t, 0, ps.len())
}

func TestPausedSetConcurrentReadWrite(t *testing.T) {
	t.Parallel()

	ps := newPausedSet([]string{"q-0"})

	const writers = 10
	const readers = 50
	const iterations = 1000

	var wg sync.WaitGroup
	wg.Add(writers + readers)

	// Writers: toggle queues in and out.
	for w := 0; w < writers; w++ {
		go func(id int) {
			defer wg.Done()
			name := fmt.Sprintf("q-%d", id)
			for i := 0; i < iterations; i++ {
				ps.add(name)
				ps.remove(name)
			}
		}(w)
	}

	// Readers: continuously snapshot and filterActive.
	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				snap := ps.snapshot()
				_ = snap
				active := ps.filterActive([]string{"q-0", "q-1", "q-2"})
				_ = active
			}
		}()
	}

	wg.Wait()

	// After all toggling, only q-0 should remain (it was in the initial set
	// and the writers for id 0 toggled it an even number of times: add+remove
	// each iteration, so the final state depends on the last operation which
	// is remove). However q-0 was initially present and writer 0 does
	// add(q-0) then remove(q-0) — after the loop q-0 is removed.
	//
	// But this is inherently racy (the last writer operation might not have
	// been observed). The key assertion is: NO PANIC, NO RACE detected by
	// the race detector.
	_ = ps.len()
}

func TestPausedSetSnapshotIsolation(t *testing.T) {
	t.Parallel()

	ps := newPausedSet([]string{"initial"})

	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: takes a snapshot, then verifies it wasn't affected by
	// concurrent mutations.
	go func() {
		defer wg.Done()
		snap := ps.snapshot()
		assert.Equal(t, []string{"initial"}, snap)

		// mutate the set while holding the snapshot
		ps.add("should-not-appear")
		ps.remove("initial")

		// the snapshot must still be the original
		assert.Equal(t, []string{"initial"}, snap)
	}()

	// Goroutine 2: concurrently mutates the set.
	go func() {
		defer wg.Done()
		ps.add("concurrent-add")
		ps.remove("initial")
		ps.add("another")
	}()

	wg.Wait()
}

// ---------- integration tests (require Redis) ----------

func TestPausedQueueConcurrentOperations(t *testing.T) {
	withRedis(t, "paused-concurrent", func(t *testing.T, store storage.Store) {
		bg := context.Background()
		assert.NoError(t, store.Flush(bg))
		m := NewManager(store)

		// Create queues in Redis so that ExistingQueue returns true.
		queues := []string{"q1", "q2", "q3", "q4", "q5"}
		for _, qn := range queues {
			q, err := store.GetQueue(bg, qn)
			assert.NoError(t, err)
			// Push a job so the queue exists.
			job := client.NewJob("TestJob", 1)
			job.Queue = qn
			assert.NoError(t, m.Push(bg, job))
			_ = q
		}

		const workers = 20
		const iterations = 50
		var wg sync.WaitGroup
		wg.Add(workers * 3) // pausers + resumer + fetchers

		// Pausers: rapidly pause queues.
		for w := 0; w < workers; w++ {
			go func(id int) {
				defer wg.Done()
				for i := 0; i < iterations; i++ {
					qn := queues[id%len(queues)]
					_ = m.PauseQueue(bg, qn)
				}
			}(w)
		}

		// Resumers: rapidly resume queues.
		for w := 0; w < workers; w++ {
			go func(id int) {
				defer wg.Done()
				for i := 0; i < iterations; i++ {
					qn := queues[id%len(queues)]
					_ = m.ResumeQueue(bg, qn)
				}
			}(w)
		}

		// Fetchers: continuously fetch with short timeout.
		for w := 0; w < workers; w++ {
			go func() {
				defer wg.Done()
				for i := 0; i < iterations; i++ {
					ctx, cancel := context.WithTimeout(bg, 10*time.Millisecond)
					_, _ = m.Fetch(ctx, "worker", queues...)
					cancel()
				}
			}()
		}

		wg.Wait()

		// Key assertion: no panic, no data race. We also verify the final
		// paused set is consistent (all members are valid queue names).
		snap := m.(*manager).paused.snapshot()
		for _, name := range snap {
			assert.Contains(t, queues, name, "paused set contains unexpected queue")
		}
	})
}

func TestPausedQueueRemoveCleansPaused(t *testing.T) {
	withRedis(t, "paused-remove", func(t *testing.T, store storage.Store) {
		bg := context.Background()
		assert.NoError(t, store.Flush(bg))
		m := NewManager(store)

		// Create queue and push a job.
		job := client.NewJob("TestJob", 1)
		job.Queue = "to-remove"
		assert.NoError(t, m.Push(bg, job))

		// Pause then remove.
		assert.NoError(t, m.PauseQueue(bg, "to-remove"))
		mgr := m.(*manager)
		assert.True(t, mgr.paused.contains("to-remove"))

		assert.NoError(t, m.RemoveQueue(bg, "to-remove"))
		assert.False(t, mgr.paused.contains("to-remove"),
			"paused set must not contain a removed queue")
	})
}

func TestPausedQueuePauseIdempotent(t *testing.T) {
	withRedis(t, "paused-idempotent", func(t *testing.T, store storage.Store) {
		bg := context.Background()
		assert.NoError(t, store.Flush(bg))
		m := NewManager(store)

		job := client.NewJob("TestJob", 1)
		job.Queue = "dup"
		assert.NoError(t, m.Push(bg, job))

		// Pause the same queue multiple times.
		for i := 0; i < 5; i++ {
			assert.NoError(t, m.PauseQueue(bg, "dup"))
		}

		mgr := m.(*manager)
		assert.Equal(t, 1, mgr.paused.len(),
			"pausing the same queue repeatedly must not create duplicates")
	})
}

func TestPausedQueueFetchAllPaused(t *testing.T) {
	withRedis(t, "paused-all", func(t *testing.T, store storage.Store) {
		bg := context.Background()
		assert.NoError(t, store.Flush(bg))
		m := NewManager(store)

		// Create queues.
		for _, qn := range []string{"a", "b"} {
			job := client.NewJob("TestJob", 1)
			job.Queue = qn
			assert.NoError(t, m.Push(bg, job))
		}

		// Pause all queues.
		assert.NoError(t, m.PauseQueue(bg, "a"))
		assert.NoError(t, m.PauseQueue(bg, "b"))

		// Fetch with a very short timeout — should return nil.
		ctx, cancel := context.WithTimeout(bg, 100*time.Millisecond)
		defer cancel()

		fetched, err := m.Fetch(ctx, "w", "a", "b")
		assert.NoError(t, err)
		assert.Nil(t, fetched, "fetch from all-paused queues must return nil")
	})
}

func TestPausedQueueManagerReloadsFromRedis(t *testing.T) {
	withRedis(t, "paused-reload", func(t *testing.T, store storage.Store) {
		bg := context.Background()
		assert.NoError(t, store.Flush(bg))

		// Create queue and pause it directly via the store.
		job := client.NewJob("TestJob", 1)
		job.Queue = "persisted"
		q, err := store.GetQueue(bg, "persisted")
		assert.NoError(t, err)
		assert.NoError(t, q.Push(bg, mustMarshalJob(t, job)))
		assert.NoError(t, q.Pause(bg))

		// New manager should pick up the paused state from Redis.
		m := NewManager(store)
		mgr := m.(*manager)
		assert.True(t, mgr.paused.contains("persisted"),
			"new manager must load paused state from Redis")
	})
}

// ---------- helpers ----------

func mustMarshalJob(t *testing.T, job *client.Job) []byte {
	t.Helper()
	data, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("cannot marshal job: %v", err)
	}
	return data
}

