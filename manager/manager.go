package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/redis/go-redis/v9"
)

const (
	// Jobs will be reserved for 30 minutes by default.
	// You can customize this per-job with the reserve_for attribute
	// in the job payload.
	DefaultTimeout = 30 * 60

	// Save dead jobs for 180 days, after that they will be purged
	DeadTTL = 180 * 24 * time.Hour
)

// A KnownError is one that returns a specific error code to the client
// such that it can be handled explicitly.  For example, the unique job feature
// will return a NOTUNIQUE error when the client tries to push() a job that already
// exists in Faktory.
//
// Unexpected errors will always use "ERR" as their code, for instance any
// malformed data, network errors, IO errors, etc.  Clients are expected to
// raise an exception for any ERR response.
type KnownError interface {
	error
	Code() string
}

type codedError struct {
	code string
	msg  string
}

func (t *codedError) Error() string {
	return fmt.Sprintf("%s %s", t.code, t.msg)
}

func (t *codedError) Code() string {
	return t.code
}

func ExpectedError(code string, msg string) error {
	return &codedError{code: code, msg: msg}
}

type Manager interface {
	Push(ctx context.Context, job *client.Job) error

	// PushBulk enqueues many jobs in one call, sharing Redis roundtrips where
	// possible. Each job is still validated and run through the push middleware
	// chain individually, but successful enqueues are grouped by queue and
	// flushed in bulk. The returned map holds an entry for every job that
	// failed (keyed by JID); a nil error means the batch itself was processed.
	PushBulk(ctx context.Context, jobs []*client.Job) (map[string]error, error)

	PauseQueue(ctx context.Context, qName string) error
	ResumeQueue(ctx context.Context, qName string) error
	RemoveQueue(ctx context.Context, qName string) error

	// Dispatch operations:
	//
	//  - Basic dequeue
	//    - Connection sends FETCH q1, q2
	//	 - Job moved from Queue into Working
	//  - Scheduled
	//	 - Job Pushed into Queue
	//	 - Job moved from Queue into Working
	//  - Failure
	//    - Job Pushed into Retries
	//  - Push
	//    - Job Pushed into Queue
	//  - Ack
	//    - Job removed from Working
	//
	// How are jobs passed to waiting workers?
	//
	// Socket sends "FETCH q1, q2, q3"
	// Connection pops each queue:
	//   store.GetQueue("q1").Pop()
	// and returns if it gets any non-nil data.
	//
	// If all nil, the connection registers itself, blocking for a job.
	Fetch(ctx context.Context, wid string, queues ...string) (*client.Job, error)

	Acknowledge(ctx context.Context, jid string) (*client.Job, error)

	Fail(ctx context.Context, fail *FailPayload) error

	// Allows arbitrary extension of a job's current reservation
	// This is a no-op if you set the time before the current
	// reservation expiry.
	ExtendReservation(ctx context.Context, jid string, until time.Time) error

	WorkingCount() int

	ReapExpiredJobs(ctx context.Context, when time.Time) (int64, error)

	// Purge deletes all dead jobs
	Purge(ctx context.Context, when time.Time) (int64, error)

	// EnqueueScheduledJobs enqueues scheduled jobs
	EnqueueScheduledJobs(ctx context.Context, when time.Time) (int64, error)

	// RetryJobs enqueues failed jobs
	RetryJobs(ctx context.Context, when time.Time) (int64, error)

	BusyCount(wid string) int

	AddMiddleware(fntype string, fn MiddlewareFunc)

	KV() storage.KV
	Redis() *redis.Client
	SetFetcher(f Fetcher)
}

func NewManager(s storage.Store) Manager {
	return newManager(s)
}

func newManager(s storage.Store) *manager {
	m := &manager{
		store:      s,
		workingMap: map[string]*Reservation{},
		pushChain:  make(MiddlewareChain, 0),
		failChain:  make(MiddlewareChain, 0),
		ackChain:   make(MiddlewareChain, 0),
		fetchChain: make(MiddlewareChain, 0),
	}
	ctx := context.Background()
	_ = m.loadWorkingSet(ctx)
	p, _ := s.PausedQueues(ctx)
	m.paused = p
	m.fetcher = BasicFetcher(m.Redis())
	return m
}

func (m *manager) SetFetcher(f Fetcher) {
	m.fetcher = f
}

func (m *manager) KV() storage.KV {
	return m.store.Raw()
}

func (m *manager) Redis() *redis.Client {
	return m.store.Redis()
}

func (m *manager) AddMiddleware(fntype string, fn MiddlewareFunc) {
	switch fntype {
	case "push":
		m.pushChain = append(m.pushChain, fn)
	case "ack":
		m.ackChain = append(m.ackChain, fn)
	case "fail":
		m.failChain = append(m.failChain, fn)
	case "fetch":
		m.fetchChain = append(m.fetchChain, fn)
	default:
		panic(fmt.Sprintf("Unknown middleware type: %s", fntype))
	}
}

type Lease interface {
	Release() error
	Payload() []byte
	Job() (*client.Job, error)
}

type manager struct {
	store storage.Store

	fetcher Fetcher

	// Hold the working set in memory so we don't need to burn CPU
	// when doing 1000s of jobs/sec.
	// When client ack's JID, we can lookup reservation
	// and remove stored entry quickly.
	workingMap   map[string]*Reservation
	pushChain    MiddlewareChain
	fetchChain   MiddlewareChain
	failChain    MiddlewareChain
	ackChain     MiddlewareChain
	paused       []string
	workingMutex sync.RWMutex
}

// prepare validates a job and applies the standard defaults shared by Push and
// PushBulk. It returns the parsed `at` time (zero when the job is immediate) so
// callers can decide between scheduling and direct enqueue without re-parsing.
func (m *manager) prepare(job *client.Job) (time.Time, error) {
	if job.Jid == "" || len(job.Jid) < 8 {
		return time.Time{}, fmt.Errorf("jobs must have a reasonable jid parameter")
	}
	if job.Type == "" {
		return time.Time{}, fmt.Errorf("jobs must have a jobtype parameter")
	}
	if job.Args == nil {
		return time.Time{}, fmt.Errorf("jobs must have an args parameter")
	}
	if job.ReserveFor > 86400 {
		return time.Time{}, fmt.Errorf("jobs cannot be reserved for more than one day")
	}

	if job.CreatedAt == "" {
		job.CreatedAt = util.Nows()
	}

	if job.Queue == "" {
		job.Queue = "default"
	}

	if job.At != "" {
		t, err := util.ParseTime(job.At)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid timestamp for 'at': %q: %w", job.At, err)
		}
		return t, nil
	}
	return time.Time{}, nil
}

func (m *manager) Push(ctx context.Context, job *client.Job) error {
	t, err := m.prepare(job)
	if err != nil {
		return err
	}

	ctxh := context.WithValue(ctx, MiddlewareHelperKey, Ctx{job, m, nil})
	err = callMiddleware(ctxh, m.pushChain, func() error {
		if job.At != "" && t.After(time.Now()) {
			data, err := json.Marshal(job)
			if err != nil {
				return fmt.Errorf("cannot marshal job payload: %w", err)
			}

			// scheduler for later
			return m.store.Scheduled().AddElement(ctx, job.At, job.Jid, data)
		}
		return m.enqueue(ctx, job)
	})
	if err != nil {
		if k, ok := err.(KnownError); ok {
			util.Infof("JID %s: %s", job.Jid, k.Error())
		}
	}
	return err
}

func (m *manager) enqueue(ctx context.Context, job *client.Job) error {
	q, err := m.store.GetQueue(ctx, job.Queue)
	if err != nil {
		return fmt.Errorf("cannot get %q queue: %w", job.Queue, err)
	}

	job.EnqueuedAt = util.Nows()
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("cannot marshal job payload: %w", err)
	}
	return q.Push(ctx, data)
}

func (m *manager) PushBulk(ctx context.Context, jobs []*client.Job) (map[string]error, error) {
	failures := map[string]error{}

	type pending struct {
		jid  string
		data []byte
	}
	// Immediate enqueues grouped by queue name, preserving submission order so
	// that a single AddBulk per queue reproduces FIFO ordering.
	batches := map[string][]pending{}

	for idx := range jobs {
		job := jobs[idx]

		t, err := m.prepare(job)
		if err != nil {
			failures[job.Jid] = err
			continue
		}

		// Run each job through the push chain individually so middleware such as
		// unique jobs keeps working; the chain's final step only buffers the
		// job, deferring the actual Redis write to the batched flush below.
		ctxh := context.WithValue(ctx, MiddlewareHelperKey, Ctx{job, m, nil})
		err = callMiddleware(ctxh, m.pushChain, func() error {
			if job.At != "" && t.After(time.Now()) {
				data, err := json.Marshal(job)
				if err != nil {
					return fmt.Errorf("cannot marshal job payload: %w", err)
				}
				// Scheduled jobs are comparatively rare; add them directly.
				return m.store.Scheduled().AddElement(ctx, job.At, job.Jid, data)
			}

			job.EnqueuedAt = util.Nows()
			data, err := json.Marshal(job)
			if err != nil {
				return fmt.Errorf("cannot marshal job payload: %w", err)
			}
			batches[job.Queue] = append(batches[job.Queue], pending{jid: job.Jid, data: data})
			return nil
		})
		if err != nil {
			if k, ok := err.(KnownError); ok {
				util.Infof("JID %s: %s", job.Jid, k.Error())
			}
			failures[job.Jid] = err
		}
	}

	for qName, items := range batches {
		q, err := m.store.GetQueue(ctx, qName)
		if err != nil {
			err = fmt.Errorf("cannot get %q queue: %w", qName, err)
			for i := range items {
				failures[items[i].jid] = err
			}
			continue
		}

		payloads := make([][]byte, len(items))
		for i := range items {
			payloads[i] = items[i].data
		}

		if err := q.AddBulk(ctx, payloads); err != nil {
			for i := range items {
				failures[items[i].jid] = err
			}
		}
	}

	return failures, nil
}
