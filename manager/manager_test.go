package manager

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestManagerBasics(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []string{"b", "c"}, filter([]string{"a"}, []string{"a", "b", "c"}))
	assert.Equal(t, []string{"a"}, filter([]string{"c", "b"}, []string{"a", "b", "c"}))
}

func TestManager(t *testing.T) {
	withRedis(t, "manager", func(t *testing.T, store storage.Store) {
		bg := context.Background()

		t.Run("Push", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			job := client.NewJob("ManagerPush", 1, 2, 3)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.Empty(t, job.EnqueuedAt)

			err = m.Push(bg, job)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, q.Size(bg))
			assert.NotEmpty(t, job.EnqueuedAt)
		})

		t.Run("PushJobWithInvalidId", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			q, err := store.GetQueue(bg, "default")
			assert.NoError(t, err)
			_, _ = q.Clear(bg)
			assert.EqualValues(t, 0, q.Size(bg))

			jids := []string{"", "id", "shortid"}
			for _, jid := range jids {
				job := client.NewJob("InvalidJob", 1, 2, 3)
				job.Queue = "default"
				job.Jid = jid
				assert.EqualValues(t, 0, q.Size(bg))
				assert.Empty(t, job.EnqueuedAt)

				err = m.Push(bg, job)

				assert.Error(t, err)
				assert.EqualValues(t, 0, q.Size(bg))
				assert.Empty(t, job.EnqueuedAt)
			}
		})

		t.Run("PushJobWithInvalidType", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			job := client.NewJob("", 1, 2, 3)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.Empty(t, job.EnqueuedAt)

			err = m.Push(bg, job)

			assert.Error(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.Empty(t, job.EnqueuedAt)
		})

		t.Run("PushJobWithoutArgs", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			job := client.NewJob("NoArgs")
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.Empty(t, job.EnqueuedAt)

			err = m.Push(bg, job)

			assert.Error(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.Empty(t, job.EnqueuedAt)
		})

		t.Run("PushScheduledJob", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			job := client.NewJob("ScheduledJob", 1, 2, 3)
			future := time.Now().Add(time.Duration(5) * time.Minute)
			job.At = util.Thens(future)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 0, store.Scheduled().Size(bg))
			assert.Empty(t, job.EnqueuedAt)

			err = m.Push(bg, job)

			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 1, store.Scheduled().Size(bg))
			assert.Empty(t, job.EnqueuedAt)
		})

		t.Run("PushScheduledJobWithPastTime", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			job := client.NewJob("ScheduledJob", 1, 2, 3)
			oneMinuteAgo := time.Now().Add(-time.Duration(1) * time.Second)
			job.At = util.Thens(oneMinuteAgo)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.Empty(t, job.EnqueuedAt)

			err = m.Push(bg, job)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, q.Size(bg))
			assert.NotEmpty(t, job.EnqueuedAt)
		})

		t.Run("PushScheduledJobWithInvalidTime", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			job := client.NewJob("ScheduledJob", 1, 2, 3)
			job.At = "invalid time"
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			_, _ = q.Clear(bg)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.Empty(t, job.EnqueuedAt)

			err = m.Push(bg, job)

			assert.Error(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.Empty(t, job.EnqueuedAt)
		})

		t.Run("Fetch", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			job := client.NewJob("ManagerPush", 1, 2, 3)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))

			err = m.Push(bg, job)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, q.Size(bg))

			queues := []string{"default"}
			fetchedJob, err := m.Fetch(context.Background(), "workerId", queues...)
			assert.NoError(t, err)
			assert.EqualValues(t, job.Jid, fetchedJob.Jid)
			assert.EqualValues(t, 0, q.Size(bg))
		})

		t.Run("EmptyFetch", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			queues := []string{}
			job, err := m.Fetch(context.Background(), "workerId", queues...)
			assert.Nil(t, job)
			assert.Error(t, err)

			q, err := store.GetQueue(bg, "default")
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			queues = []string{"default"}
			fetchedJob, err := m.Fetch(ctx, "workerId", queues...)
			assert.NoError(t, err)
			assert.Nil(t, fetchedJob)
		})

		t.Run("FetchWithPause", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))

			dq, err := store.GetQueue(bg, "default")
			assert.NoError(t, err)
			assert.NoError(t, dq.Pause(bg))

			m := NewManager(store)

			job := client.NewJob("ManagerPush", 1, 2, 3)
			q1, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q1.Size(bg))

			err = m.Push(bg, job)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, q1.Size(bg))

			email := client.NewJob("SendEmail", 1, 2, 3)
			email.Queue = "email"
			q2, err := store.GetQueue(bg, email.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q2.Size(bg))

			err = m.Push(bg, email)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, q2.Size(bg))

			queues := []string{"default", "email"}

			fetchedJob, err := m.Fetch(bg, "workerId", queues...)
			assert.NoError(t, err)
			assert.NotNil(t, fetchedJob)
			assert.EqualValues(t, email.Jid, fetchedJob.Jid)
			assert.EqualValues(t, 1, q1.Size(bg))
			assert.EqualValues(t, 0, q2.Size(bg))

			assert.NoError(t, m.ResumeQueue(bg, "default"))

			fetchedJob, err = m.Fetch(bg, "workerId", queues...)
			assert.NoError(t, err)
			assert.NotNil(t, fetchedJob)
			assert.EqualValues(t, job.Jid, fetchedJob.Jid)
			assert.EqualValues(t, 0, q1.Size(bg))
			assert.EqualValues(t, 0, q2.Size(bg))

			pq, err := store.PausedQueues(bg)
			assert.NoError(t, err)
			assert.Equal(t, []string{}, pq)

			assert.NoError(t, m.PauseQueue(bg, "default"))

			pq, err = store.PausedQueues(bg)
			assert.NoError(t, err)
			assert.Equal(t, []string{"default"}, pq)
		})

		t.Run("FetchFromMultipleQueues", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			job := client.NewJob("ManagerPush", 1, 2, 3)
			q1, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q1.Size(bg))

			err = m.Push(bg, job)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, q1.Size(bg))

			email := client.NewJob("SendEmail", 1, 2, 3)
			email.Queue = "email"
			q2, err := store.GetQueue(bg, email.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q2.Size(bg))

			err = m.Push(bg, email)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, q2.Size(bg))

			queues := []string{"default", "email"}

			fetchedJob, err := m.Fetch(context.Background(), "workerId", queues...)
			assert.NoError(t, err)
			assert.NotNil(t, fetchedJob)
			assert.EqualValues(t, job.Jid, fetchedJob.Jid)
			assert.EqualValues(t, 0, q1.Size(bg))
			assert.EqualValues(t, 1, q2.Size(bg))

			fetchedJob, err = m.Fetch(context.Background(), "workerId", queues...)
			assert.NoError(t, err)
			assert.NotNil(t, fetchedJob)
			assert.EqualValues(t, email.Jid, fetchedJob.Jid)
			assert.EqualValues(t, 0, q1.Size(bg))
			assert.EqualValues(t, 0, q2.Size(bg))
		})

		t.Run("FetchAwaitsForNewJob", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			q, err := store.GetQueue(bg, "default")
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			go func() {
				time.Sleep(time.Duration(1) * time.Second)

				t.Log("Pushing job")
				job := client.NewJob("ManagerPush", 1, 2, 3)
				err = m.Push(bg, job)
				assert.NoError(t, err)
			}()

			queues := []string{"default"}
			fetchedJob, err := m.Fetch(ctx, "workerId", queues...)
			assert.NoError(t, err)
			assert.NotEmpty(t, fetchedJob)
		})
	})
}

func TestPushBulk(t *testing.T) {
	withRedis(t, "pushbulk", func(t *testing.T, store storage.Store) {
		bg := context.Background()

		t.Run("AllSucceed", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			q, err := store.GetQueue(bg, "default")
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))

			jobs := make([]*client.Job, 0, 5)
			for i := 0; i < 5; i++ {
				jobs = append(jobs, client.NewJob("BulkJob", i))
			}

			failures, err := m.PushBulk(bg, jobs)
			assert.NoError(t, err)
			assert.Empty(t, failures)
			assert.EqualValues(t, 5, q.Size(bg))
			for _, job := range jobs {
				assert.NotEmpty(t, job.EnqueuedAt)
			}
		})

		t.Run("PartialFailure", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			q, err := store.GetQueue(bg, "default")
			assert.NoError(t, err)

			good1 := client.NewJob("Good", 1)
			good2 := client.NewJob("Good", 2)

			noType := client.NewJob("Good", 3)
			noType.Type = ""

			shortJid := client.NewJob("Good", 4)
			shortJid.Jid = "short"

			noArgs := client.NewJob("NoArgs") // variadic with no args leaves Args nil

			jobs := []*client.Job{good1, noType, good2, shortJid, noArgs}

			failures, err := m.PushBulk(bg, jobs)
			assert.NoError(t, err)

			// only the three invalid jobs are reported, keyed by JID
			assert.Len(t, failures, 3)
			assert.EqualError(t, failures[noType.Jid], "jobs must have a jobtype parameter")
			assert.EqualError(t, failures[shortJid.Jid], "jobs must have a reasonable jid parameter")
			assert.EqualError(t, failures[noArgs.Jid], "jobs must have an args parameter")

			// the two valid jobs are still enqueued despite their siblings failing
			assert.EqualValues(t, 2, q.Size(bg))
			assert.NotEmpty(t, good1.EnqueuedAt)
			assert.NotEmpty(t, good2.EnqueuedAt)
		})

		t.Run("MultipleQueues", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			dq, err := store.GetQueue(bg, "default")
			assert.NoError(t, err)
			eq, err := store.GetQueue(bg, "email")
			assert.NoError(t, err)

			jobs := make([]*client.Job, 0)
			for i := 0; i < 3; i++ {
				jobs = append(jobs, client.NewJob("DefaultJob", i))
			}
			for i := 0; i < 2; i++ {
				j := client.NewJob("EmailJob", i)
				j.Queue = "email"
				jobs = append(jobs, j)
			}

			failures, err := m.PushBulk(bg, jobs)
			assert.NoError(t, err)
			assert.Empty(t, failures)
			assert.EqualValues(t, 3, dq.Size(bg))
			assert.EqualValues(t, 2, eq.Size(bg))
		})

		t.Run("ScheduledJobs", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			q, err := store.GetQueue(bg, "default")
			assert.NoError(t, err)
			assert.EqualValues(t, 0, store.Scheduled().Size(bg))

			future := util.Thens(time.Now().Add(5 * time.Minute))
			jobs := make([]*client.Job, 0)
			for i := 0; i < 4; i++ {
				j := client.NewJob("Later", i)
				j.At = future
				jobs = append(jobs, j)
			}

			failures, err := m.PushBulk(bg, jobs)
			assert.NoError(t, err)
			assert.Empty(t, failures)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 4, store.Scheduled().Size(bg))
			for _, job := range jobs {
				// scheduled jobs are not enqueued, so they have no EnqueuedAt yet
				assert.Empty(t, job.EnqueuedAt)
			}
		})

		t.Run("Empty", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			failures, err := m.PushBulk(bg, []*client.Job{})
			assert.NoError(t, err)
			assert.Empty(t, failures)

			failures, err = m.PushBulk(bg, nil)
			assert.NoError(t, err)
			assert.Empty(t, failures)
		})

		t.Run("PreservesFifoOrder", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := NewManager(store)

			first := client.NewJob("Ordered", "first")
			second := client.NewJob("Ordered", "second")
			third := client.NewJob("Ordered", "third")

			failures, err := m.PushBulk(bg, []*client.Job{first, second, third})
			assert.NoError(t, err)
			assert.Empty(t, failures)

			f1, err := m.Fetch(bg, "wid", "default")
			assert.NoError(t, err)
			assert.EqualValues(t, first.Jid, f1.Jid)

			f2, err := m.Fetch(bg, "wid", "default")
			assert.NoError(t, err)
			assert.EqualValues(t, second.Jid, f2.Jid)

			f3, err := m.Fetch(bg, "wid", "default")
			assert.NoError(t, err)
			assert.EqualValues(t, third.Jid, f3.Jid)
		})
	})
}

func withRedis(t *testing.T, name string, fn func(*testing.T, storage.Store)) {
	t.Parallel()

	dir := fmt.Sprintf("/tmp/faktory-test-%s", name)
	defer os.RemoveAll(dir)

	sock := fmt.Sprintf("%s/redis.sock", dir)
	stopper, err := storage.Boot(dir, sock)
	if err != nil {
		panic(err)
	}
	defer func() { _ = stopper() }()

	store, err := storage.Open(sock, 10)
	if err != nil {
		panic(err)
	}
	defer store.Close()

	fn(t, store)

}
