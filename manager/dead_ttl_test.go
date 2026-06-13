package manager

import (
	"context"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/stretchr/testify/assert"
)

func TestDefaultDeadTTL(t *testing.T) {
	withRedis(t, "dead-ttl", func(t *testing.T, store storage.Store) {
		bg := context.Background()

		t.Run("DefaultTTL", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			// New manager should use DefaultDeadTTL
			assert.Equal(t, DefaultDeadTTL, m.DeadTTL())
			assert.Equal(t, 180*24*time.Hour, m.DeadTTL())
		})

		t.Run("SetAndGetDeadTTL", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			customTTL := 90 * 24 * time.Hour
			m.SetDeadTTL(customTTL)
			assert.Equal(t, customTTL, m.DeadTTL())

			// Can be changed again
			newTTL := 30 * 24 * time.Hour
			m.SetDeadTTL(newTTL)
			assert.Equal(t, newTTL, m.DeadTTL())
		})

		t.Run("SendToMorgueUsesCustomTTL", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			// Set a short custom TTL so we can verify it's used
			customTTL := 5 * time.Minute
			m.SetDeadTTL(customTTL)

			job := client.NewJob("DeadJob", 1, 2, 3)
			retries := 1
			job.Retry = &retries

			lease := &simpleLease{job: job}
			err := m.reserve(bg, "workerId", lease)
			assert.NoError(t, err)

			// First fail: goes to retries
			fail := failure(job.Jid, "oops", "SomeError", nil)
			err = m.Fail(bg, fail)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Retries().Size(bg))
			assert.EqualValues(t, 0, store.Dead().Size(bg))

			// Reserve the retry
			err = m.reserve(bg, "workerId", lease)
			assert.NoError(t, err)

			// Second fail: retries exhausted, goes to morgue
			before := time.Now()
			fail = failure(job.Jid, "oops again", "AnotherError", nil)
			err = m.Fail(bg, fail)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Retries().Size(bg))
			assert.EqualValues(t, 1, store.Dead().Size(bg))

			// Verify the dead job's expiry matches the custom TTL
			entry, err := store.Dead().Get(bg, []byte(job.Jid))
			assert.NoError(t, err)
			assert.NotNil(t, entry)

			expiryStr, err := entry.Key()
			assert.NoError(t, err)
			expiry, err := parseTimestamp(expiryStr)
			assert.NoError(t, err)

			expectedExpiry := before.Add(customTTL)
			// Allow 2 second tolerance for test execution time
			diff := expiry.Sub(expectedExpiry)
			assert.InDelta(t, 0, diff.Seconds(), 2,
				"dead job expiry should be ~%s from now, got %v", customTTL, diff)
		})

		t.Run("SendToMorgueUsesDefaultTTL", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			// Don't change TTL, use default (180 days)
			assert.Equal(t, DefaultDeadTTL, m.DeadTTL())

			job := client.NewJob("DeadJob", 1, 2, 3)
			retries := 1
			job.Retry = &retries

			lease := &simpleLease{job: job}
			err := m.reserve(bg, "workerId", lease)
			assert.NoError(t, err)

			// First fail -> retries
			fail := failure(job.Jid, "oops", "SomeError", nil)
			err = m.Fail(bg, fail)
			assert.NoError(t, err)

			// Reserve retry
			err = m.reserve(bg, "workerId", lease)
			assert.NoError(t, err)

			// Second fail -> morgue with default TTL
			before := time.Now()
			fail = failure(job.Jid, "oops again", "AnotherError", nil)
			err = m.Fail(bg, fail)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Dead().Size(bg))

			// Verify the dead job's expiry matches the default TTL (180 days)
			entry, err := store.Dead().Get(bg, []byte(job.Jid))
			assert.NoError(t, err)

			expiryStr, err := entry.Key()
			assert.NoError(t, err)
			expiry, err := parseTimestamp(expiryStr)
			assert.NoError(t, err)

			expectedExpiry := before.Add(DefaultDeadTTL)
			diff := expiry.Sub(expectedExpiry)
			assert.InDelta(t, 0, diff.Seconds(), 2,
				"dead job expiry should be ~180 days from now")
		})

		t.Run("TTLChangeAffectsNewDeadJobs", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			// Start with a short TTL
			shortTTL := 10 * time.Minute
			m.SetDeadTTL(shortTTL)

			job1 := client.NewJob("DeadJob1", 1, 2, 3)
			retries := 1
			job1.Retry = &retries

			lease1 := &simpleLease{job: job1}
			err := m.reserve(bg, "workerId1", lease1)
			assert.NoError(t, err)

			// Fail twice to exhaust retries
			fail := failure(job1.Jid, "oops", "Err", nil)
			err = m.Fail(bg, fail)
			assert.NoError(t, err)
			err = m.reserve(bg, "workerId1", lease1)
			assert.NoError(t, err)
			beforeFirst := time.Now()
			fail = failure(job1.Jid, "oops2", "Err", nil)
			err = m.Fail(bg, fail)
			assert.NoError(t, err)

			// Now change the TTL (simulating a SIGHUP reload)
			longTTL := 365 * 24 * time.Hour
			m.SetDeadTTL(longTTL)

			// Send another job to morgue
			job2 := client.NewJob("DeadJob2", 4, 5, 6)
			job2.Retry = &retries

			lease2 := &simpleLease{job: job2}
			err = m.reserve(bg, "workerId2", lease2)
			assert.NoError(t, err)
			fail = failure(job2.Jid, "oops", "Err", nil)
			err = m.Fail(bg, fail)
			assert.NoError(t, err)
			err = m.reserve(bg, "workerId2", lease2)
			assert.NoError(t, err)
			beforeSecond := time.Now()
			fail = failure(job2.Jid, "oops2", "Err", nil)
			err = m.Fail(bg, fail)
			assert.NoError(t, err)

			assert.EqualValues(t, 2, store.Dead().Size(bg))

			// Verify first job uses the short TTL
			entry1, err := store.Dead().Get(bg, []byte(job1.Jid))
			assert.NoError(t, err)
			expiryStr1, _ := entry1.Key()
			expiry1, _ := parseTimestamp(expiryStr1)
			expectedExpiry1 := beforeFirst.Add(shortTTL)
			diff1 := expiry1.Sub(expectedExpiry1)
			assert.InDelta(t, 0, diff1.Seconds(), 2,
				"first dead job should use short TTL")

			// Verify second job uses the long TTL
			entry2, err := store.Dead().Get(bg, []byte(job2.Jid))
			assert.NoError(t, err)
			expiryStr2, _ := entry2.Key()
			expiry2, _ := parseTimestamp(expiryStr2)
			expectedExpiry2 := beforeSecond.Add(longTTL)
			diff2 := expiry2.Sub(expectedExpiry2)
			assert.InDelta(t, 0, diff2.Seconds(), 2,
				"second dead job should use long TTL after config change")
		})
	})
}

// parseTimestamp parses a timestamp string produced by util.Thens.
func parseTimestamp(s string) (time.Time, error) {
	return time.Parse("2006-01-02T15:04:05.000Z", s)
}
