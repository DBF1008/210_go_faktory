package manager

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestRetry(t *testing.T) {
	withRedis(t, "retry", func(t *testing.T, store storage.Store) {
		bg := context.Background()

		t.Run("fail", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			job := client.NewJob("ManagerPush", 1, 2, 3)
			retries := 1
			job.Retry = &retries

			lease := &simpleLease{job: job}

			err := m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())
			assert.NotNil(t, m.workingMap[job.Jid])
			assert.Nil(t, m.workingMap[job.Jid].Job.Failure)
			assert.EqualValues(t, 0, store.Retries().Size(bg))
			assert.EqualValues(t, 0, store.TotalProcessed(bg))
			assert.EqualValues(t, 0, store.TotalFailures(bg))
			assert.False(t, lease.released)

			fail := failure(job.Jid, "uh no", "SomeError", nil)
			err = m.Fail(bg, fail)

			assert.NoError(t, err)
			assert.Nil(t, m.workingMap[job.Jid])
			assert.EqualValues(t, 1, store.Retries().Size(bg))
			assert.EqualValues(t, 1, store.TotalProcessed(bg))
			assert.EqualValues(t, 1, store.TotalFailures(bg))
			assert.True(t, lease.released)

			// retry job
			err = m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())
			assert.NotNil(t, m.workingMap[job.Jid])
			assert.NotNil(t, m.workingMap[job.Jid].Job.Failure)
			assert.EqualValues(t, 1, store.Retries().Size(bg))
			assert.EqualValues(t, 0, store.Dead().Size(bg))

			fail = failure(job.Jid, "uh no again", "YetAnotherError", nil)
			err = m.Fail(bg, fail)

			assert.NoError(t, err)
			assert.Nil(t, m.workingMap[job.Jid])
			assert.EqualValues(t, 1, store.Retries().Size(bg))
			assert.EqualValues(t, 1, store.Dead().Size(bg))
			assert.EqualValues(t, 2, store.TotalProcessed(bg))
			assert.EqualValues(t, 2, store.TotalFailures(bg))
		})

		t.Run("FailOneShotJob", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			job := client.NewJob("ManagerPush", 1, 2, 3)
			retries := 0
			job.Retry = &retries

			lease := &simpleLease{job: job}
			err := m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())
			assert.NotNil(t, m.workingMap[job.Jid])
			assert.Nil(t, m.workingMap[job.Jid].Job.Failure)
			assert.EqualValues(t, 0, store.Retries().Size(bg))
			assert.EqualValues(t, 0, store.TotalProcessed(bg))
			assert.EqualValues(t, 0, store.TotalFailures(bg))

			fail := failure(job.Jid, "uh no", "SomeError", nil)
			err = m.Fail(bg, fail)

			assert.NoError(t, err)
			assert.Nil(t, m.workingMap[job.Jid])
			assert.EqualValues(t, 0, store.Retries().Size(bg))
			assert.EqualValues(t, 0, store.Dead().Size(bg))
			assert.EqualValues(t, 1, store.TotalProcessed(bg))
			assert.EqualValues(t, 1, store.TotalFailures(bg))
		})

		t.Run("FailWithInvalidFailPayload", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			err := m.Fail(bg, nil)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "missing failure info")

			err = m.Fail(bg, &FailPayload{})
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "missing JID")

			err = m.Fail(bg, &FailPayload{Jid: "1238123123"})
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "not found")
		})
	})
}

func failure(jid, msg, errtype string, bt []string) *FailPayload {
	var f FailPayload
	f.Jid = jid
	f.ErrorMessage = msg
	f.ErrorType = errtype
	f.Backtrace = bt
	return &f
}

func TestDeadRetention(t *testing.T) {
	withRedis(t, "deadttl", func(t *testing.T, store storage.Store) {
		bg := context.Background()

		t.Run("defaults to DefaultDeadTTL", func(t *testing.T) {
			m := newManager(store)
			assert.Equal(t, DefaultDeadTTL, m.DeadTTL())
		})

		t.Run("SetDeadTTL updates the retention", func(t *testing.T) {
			m := newManager(store)
			m.SetDeadTTL(48 * time.Hour)
			assert.Equal(t, 48*time.Hour, m.DeadTTL())
		})

		t.Run("sendToMorgue honors the configured TTL", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)
			ttl := 36 * time.Hour
			m.SetDeadTTL(ttl)

			job := client.NewJob("DeadJob", 1, 2, 3)
			before := time.Now()
			assert.NoError(t, m.sendToMorgue(bg, job))
			after := time.Now()

			assert.EqualValues(t, 1, store.Dead().Size(bg))
			expiry := firstDeadExpiry(t, bg, store)
			assert.WithinRange(t, expiry,
				before.Add(ttl).Add(-2*time.Second),
				after.Add(ttl).Add(2*time.Second))
		})

		t.Run("exhausting retries uses the configured TTL", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)
			ttl := 12 * time.Hour
			m.SetDeadTTL(ttl)

			job := client.NewJob("DeadJob", 1)
			retries := 1
			job.Retry = &retries
			// Pre-populate a Failure that is already on its last attempt so a
			// single Fail exhausts retries and routes the job to the morgue.
			job.Failure = &client.Failure{
				RetryCount:     1,
				RetryRemaining: 0,
				FailedAt:       util.Nows(),
			}

			lease := &simpleLease{job: job}
			assert.NoError(t, m.reserve(bg, "workerId", lease))

			before := time.Now()
			assert.NoError(t, m.Fail(bg, failure(job.Jid, "boom", "Err", nil)))
			after := time.Now()

			assert.EqualValues(t, 0, store.Retries().Size(bg))
			assert.EqualValues(t, 1, store.Dead().Size(bg))
			expiry := firstDeadExpiry(t, bg, store)
			assert.WithinRange(t, expiry,
				before.Add(ttl).Add(-2*time.Second),
				after.Add(ttl).Add(2*time.Second))
		})
	})
}

// firstDeadExpiry returns the expiry timestamp encoded in the first entry of
// the dead set, decoded from its "timestamp|jid" key.
func firstDeadExpiry(t *testing.T, ctx context.Context, store storage.Store) time.Time {
	var out time.Time
	_, err := store.Dead().Page(ctx, 0, 10, func(idx int, e storage.SortedEntry) error {
		key, err := e.Key()
		if err != nil {
			return err
		}
		ts, _, _ := strings.Cut(string(key), "|")
		tm, err := util.ParseTime(ts)
		if err != nil {
			return err
		}
		out = tm
		return nil
	})
	assert.NoError(t, err)
	return out
}
