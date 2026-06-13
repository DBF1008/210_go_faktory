package manager

import (
	"context"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestLoadWorkingSet(t *testing.T) {
	withRedis(t, "working", func(t *testing.T, store storage.Store) {
		bg := context.Background()
		t.Run("LoadWorkingSet", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			job := client.NewJob("WorkingJob", 1, 2, 3)
			job.ReserveFor = 600
			assert.EqualValues(t, 0, store.Working().Size(bg))
			assert.EqualValues(t, 0, m.WorkingCount())

			lease := &simpleLease{job: job}
			err := m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())

			m2 := newManager(store)
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m2.WorkingCount())
		})

		t.Run("ManagerReserve", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			job := client.NewJob("WorkingJob", 1, 2, 3)
			job.ReserveFor = 600
			assert.EqualValues(t, 0, store.Working().Size(bg))
			assert.EqualValues(t, 0, m.WorkingCount())

			lease := &simpleLease{job: job}
			err := m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())
		})

		t.Run("ReserveWithInvalidTimeout", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			timeouts := []int{0, 20, 50, 59, 86401, 100000}
			for _, timeout := range timeouts {
				job := client.NewJob("InvalidJob", 1, 2, 3)
				job.ReserveFor = timeout
				assert.EqualValues(t, 0, store.Working().Size(bg))

				// doesn't return an error but resets to default timeout
				lease := &simpleLease{job: job}
				err := m.reserve(bg, "workerId", lease)

				assert.NoError(t, err)
				assert.EqualValues(t, 1, store.Working().Size(bg))
				err = store.Working().Clear(bg)
				assert.NoError(t, err)
			}
		})

		t.Run("ManagerAcknowledge", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			job, err := m.Acknowledge(bg, "")
			assert.NoError(t, err)
			assert.Nil(t, job)

			job = client.NewJob("AckJob", 1, 2, 3)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 0, store.Working().Size(bg))
			assert.EqualValues(t, 0, m.WorkingCount())
			assert.EqualValues(t, 0, store.TotalProcessed(bg))
			assert.EqualValues(t, 0, store.TotalFailures(bg))

			lease := &simpleLease{job: job}
			err = m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())
			assert.EqualValues(t, 0, store.TotalProcessed(bg))
			assert.EqualValues(t, 0, store.TotalFailures(bg))
			assert.False(t, lease.released)

			assert.EqualValues(t, 1, m.BusyCount("workerId"))
			assert.EqualValues(t, 0, m.BusyCount("fakeId"))

			aJob, err := m.Acknowledge(bg, job.Jid)
			assert.NoError(t, err)
			assert.Equal(t, job.Jid, aJob.Jid)
			assert.EqualValues(t, 1, store.TotalProcessed(bg))
			assert.EqualValues(t, 0, store.TotalFailures(bg))
			assert.EqualValues(t, 0, m.BusyCount("workerId"))
			assert.True(t, lease.released)

			aJob, err = m.Acknowledge(bg, job.Jid)
			assert.NoError(t, err)
			assert.Nil(t, aJob)
			assert.EqualValues(t, 1, store.TotalProcessed(bg))
			assert.EqualValues(t, 0, store.TotalFailures(bg))
		})

		t.Run("ManagerReapExpiredJobs", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			job := client.NewJob("WorkingJob", 1, 2, 3)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 0, store.Working().Size(bg))
			assert.EqualValues(t, 0, m.WorkingCount())

			lease := &simpleLease{job: job}
			err = m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())

			exp := time.Now().Add(time.Duration(10) * time.Second)
			count, err := m.ReapExpiredJobs(bg, exp)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, count)
			assert.EqualValues(t, 0, store.Retries().Size(bg))

			err = m.ExtendReservation(bg, "nosuch", time.Now().Add(50*time.Hour))
			assert.NoError(t, err)

			util.LogInfo = true
			util.LogDebug = true
			util.Infof("Extending %s", job.Jid)
			err = m.ExtendReservation(bg, job.Jid, time.Now().Add(50*time.Hour))
			assert.NoError(t, err)

			exp = time.Now().Add(time.Duration(DefaultTimeout+10) * time.Second)
			count, err = m.ReapExpiredJobs(bg, exp)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, count)
			assert.EqualValues(t, 0, store.Retries().Size(bg))

			exp = time.Now().Add(51 * time.Hour)
			count, err = m.ReapExpiredJobs(bg, exp)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, count)
			assert.EqualValues(t, 1, store.Retries().Size(bg))
		})

		t.Run("ReloadRestoresReservationTimes", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			job := client.NewJob("ReloadJob", 1, 2, 3)
			job.ReserveFor = 600
			lease := &simpleLease{job: job}
			assert.NoError(t, m.reserve(bg, "workerId", lease))

			orig := m.workingMap[job.Jid]
			assert.False(t, orig.ReservedAt().IsZero())
			assert.False(t, orig.ExpiresAt().IsZero())
			wantSince := orig.Since
			wantExpiry := orig.Expiry

			// simulate a server restart: a fresh manager reloads the working set
			m2 := newManager(store)
			reloaded := m2.workingMap[job.Jid]
			assert.NotNil(t, reloaded)

			// the internal time fields must be rehydrated from the serialized form,
			// not left at their zero value.
			assert.False(t, reloaded.ReservedAt().IsZero())
			assert.False(t, reloaded.ExpiresAt().IsZero())
			assert.Equal(t, wantSince, util.Thens(reloaded.ReservedAt()))
			assert.Equal(t, wantExpiry, util.Thens(reloaded.ExpiresAt()))
			assert.Equal(t, wantSince, reloaded.Since)
			assert.Equal(t, wantExpiry, reloaded.Expiry)
		})

		t.Run("ExtendedReservationSurvivesRestart", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			job := client.NewJob("ExtendedJob", 1, 2, 3)
			job.ReserveFor = 600
			lease := &simpleLease{job: job}
			assert.NoError(t, m.reserve(bg, "workerId", lease))
			assert.EqualValues(t, 1, store.Working().Size(bg))

			// the worker requests a much later deadline
			extendUntil := time.Now().Add(50 * time.Hour)
			assert.NoError(t, m.ExtendReservation(bg, job.Jid, extendUntil))
			wantExpiry := util.Thens(extendUntil)

			// reaping past the original 600s window triggers the in-memory
			// auto-extension, which must rewrite the persisted payload rather
			// than re-storing the stale one.
			reapAt := time.Now().Add(601 * time.Second)
			count, err := m.ReapExpiredJobs(bg, reapAt)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, count)
			assert.EqualValues(t, 1, store.Working().Size(bg))

			// the persisted payload's expires_at must match the new deadline,
			// keeping it consistent with the sorted-set score.
			var stored Reservation
			assert.NoError(t, store.Working().Each(bg, func(idx int, e storage.SortedEntry) error {
				return util.JsonUnmarshal(e.Value(), &stored)
			}))
			assert.Equal(t, wantExpiry, stored.Expiry)

			// simulate a restart and confirm the extended deadline came back intact
			m2 := newManager(store)
			reloaded := m2.workingMap[job.Jid]
			assert.NotNil(t, reloaded)
			assert.Equal(t, wantExpiry, reloaded.Expiry)
			assert.Equal(t, wantExpiry, util.Thens(reloaded.ExpiresAt()))

			// ACK after restart must locate and remove the element using the
			// extended timestamp; with a stale payload the score would not match
			// and the job would leak in the working set.
			aJob, err := m2.Acknowledge(bg, job.Jid)
			assert.NoError(t, err)
			assert.NotNil(t, aJob)
			assert.Equal(t, job.Jid, aJob.Jid)
			assert.EqualValues(t, 0, store.Working().Size(bg))
			assert.EqualValues(t, 0, m2.WorkingCount())
		})
	})
}
