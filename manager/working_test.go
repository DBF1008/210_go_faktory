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

		t.Run("LoadWorkingSetRestoresTimeFields", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			job := client.NewJob("TimeFieldsJob", 1, 2, 3)
			job.ReserveFor = 600
			lease := &simpleLease{job: job}
			err := m.reserve(bg, "workerId", lease)
			assert.NoError(t, err)

			// Simulate restart
			m2 := newManager(store)
			assert.EqualValues(t, 1, m2.WorkingCount())

			m2.workingMutex.RLock()
			res, ok := m2.workingMap[job.Jid]
			m2.workingMutex.RUnlock()
			assert.True(t, ok)

			// tsince must be restored from the Since string
			assert.False(t, res.tsince.IsZero(), "tsince should be restored from reserved_at")
			// texpiry must be restored from the Expiry string
			assert.False(t, res.texpiry.IsZero(), "texpiry should be restored from expires_at")

			// The parsed values must match the serialized strings
			assert.Equal(t, res.Since, util.Thens(res.tsince))
			assert.Equal(t, res.Expiry, util.Thens(res.texpiry))

			// texpiry should be approximately Since + 600s
			diff := res.texpiry.Sub(res.tsince)
			assert.InDelta(t, 600, diff.Seconds(), 2)
		})

		t.Run("ExtensionSurvivesRestart", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			job := client.NewJob("ExtendRestartJob", 1, 2, 3)
			job.ReserveFor = 60
			lease := &simpleLease{job: job}
			err := m.reserve(bg, "workerId", lease)
			assert.NoError(t, err)

			extendUntil := time.Now().Add(2 * time.Hour)
			err = m.ExtendReservation(bg, job.Jid, extendUntil)
			assert.NoError(t, err)

			// Trigger auto-extend in the reaper so the new Expiry is persisted
			exp := time.Now().Add(time.Duration(DefaultTimeout+10) * time.Second)
			count, err := m.ReapExpiredJobs(bg, exp)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, count)
			assert.EqualValues(t, 1, store.Working().Size(bg))

			// Simulate restart
			m2 := newManager(store)
			assert.EqualValues(t, 1, m2.WorkingCount())

			m2.workingMutex.RLock()
			res, ok := m2.workingMap[job.Jid]
			m2.workingMutex.RUnlock()
			assert.True(t, ok)

			// Extension field should be restored
			assert.False(t, res.Extension.IsZero(), "Extension should survive restart")
			assert.WithinDuration(t, extendUntil, res.Extension, time.Second)

			// texpiry should reflect the extended time
			assert.WithinDuration(t, extendUntil, res.texpiry, time.Second)

			// Expiry string should match the extension
			parsedExpiry, err := util.ParseTime(res.Expiry)
			assert.NoError(t, err)
			assert.WithinDuration(t, extendUntil, parsedExpiry, time.Second)

			// Reaping at the original expiry time should NOT reap the job
			originalExpiry := res.tsince.Add(60 * time.Second)
			count, err = m2.ReapExpiredJobs(bg, originalExpiry.Add(time.Minute))
			assert.NoError(t, err)
			assert.EqualValues(t, 0, count, "job should not be reaped before extended expiry")

			// Reaping past the extended expiry should reap
			count, err = m2.ReapExpiredJobs(bg, extendUntil.Add(time.Hour))
			assert.NoError(t, err)
			assert.EqualValues(t, 1, count)
		})

		t.Run("AckAfterRestartWithExtension", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			job := client.NewJob("AckExtendJob", 1, 2, 3)
			job.ReserveFor = 60
			lease := &simpleLease{job: job}
			err := m.reserve(bg, "workerId", lease)
			assert.NoError(t, err)

			extendUntil := time.Now().Add(2 * time.Hour)
			err = m.ExtendReservation(bg, job.Jid, extendUntil)
			assert.NoError(t, err)

			// Trigger auto-extend so the new Expiry is persisted to Redis
			exp := time.Now().Add(time.Duration(DefaultTimeout+10) * time.Second)
			count, err := m.ReapExpiredJobs(bg, exp)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, count)

			// Simulate restart
			m2 := newManager(store)
			assert.EqualValues(t, 1, m2.WorkingCount())
			assert.EqualValues(t, 1, store.Working().Size(bg))

			// ACK should succeed: Expiry in memory matches sorted set score
			aJob, err := m2.Acknowledge(bg, job.Jid)
			assert.NoError(t, err)
			assert.NotNil(t, aJob)
			assert.Equal(t, job.Jid, aJob.Jid)

			// Working set should be empty
			assert.EqualValues(t, 0, m2.WorkingCount())
			assert.EqualValues(t, 0, store.Working().Size(bg))
		})
	})
}
