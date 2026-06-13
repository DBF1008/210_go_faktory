package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

var (
	JobReservationExpired = &FailPayload{
		ErrorType:    "ReservationExpired",
		ErrorMessage: "Faktory job reservation expired",
	}
)

type Reservation struct {
	tsince    time.Time
	texpiry   time.Time
	extension time.Time
	lease     Lease
	Job       *client.Job `json:"job"`
	Since     string      `json:"reserved_at"`
	Expiry    string      `json:"expires_at"`
	Wid       string      `json:"wid"`
	Extension time.Time   `json:"extended_until,omitempty"`
}

func (res *Reservation) ReservedAt() time.Time {
	return res.tsince
}

func (res *Reservation) ExpiresAt() time.Time {
	return res.texpiry
}

func (m *manager) ExtendReservation(ctx context.Context, jid string, until time.Time) error {
	m.workingMutex.Lock()
	if localres, ok := m.workingMap[jid]; ok {
		if localres.texpiry.Before(until) {
			localres.extension = until
			localres.Extension = until
		}
	}
	m.workingMutex.Unlock()
	return nil
}

func (m *manager) WorkingCount() int {
	m.workingMutex.RLock()
	defer m.workingMutex.RUnlock()
	return len(m.workingMap)
}

func (m *manager) BusyCount(wid string) int {
	m.workingMutex.RLock()
	defer m.workingMutex.RUnlock()

	count := 0
	for _, res := range m.workingMap {
		if res.Wid == wid {
			count++
		}
	}

	return count
}

/*
 * When we restart the server, we need to load the
 * current set of Reservations back into memory so any
 * outstanding jobs can be Acknowledged successfully.
 *
 * The alternative is that a server restart would re-execute
 * all outstanding jobs, something to be avoided when possible.
 */
func (m *manager) loadWorkingSet(ctx context.Context) error {
	m.workingMutex.Lock()
	defer m.workingMutex.Unlock()

	addedCount := 0
	// Track entries whose sorted set score needs updating because
	// a persisted extension changed the effective expiry.
	type scoreUpdate struct {
		entry   storage.SortedEntry
		newData []byte
		expiry  string
	}
	var updates []scoreUpdate

	err := m.store.Working().Each(ctx, func(idx int, entry storage.SortedEntry) error {
		var res Reservation
		err := util.JsonUnmarshal(entry.Value(), &res)
		if err != nil {
			//  We can't return an error here, this method is best effort
			// as we are booting the server. We can't allow corrupted data
			// to stop Faktory from starting.
			util.Error("Unable to restore working job", err)
			return nil
		}

		// Restore unexported time fields from their serialized string forms.
		// Without this, tsince/texpiry remain zero-valued after restart,
		// causing ACK/FAIL to use stale or zero expiry for sorted set lookups.
		if res.Since != "" {
			if t, err := util.ParseTime(res.Since); err == nil {
				res.tsince = t
			} else {
				util.Warnf("Unable to parse reserved_at for %s: %v", res.Job.Jid, err)
			}
		}
		if res.Expiry != "" {
			if t, err := util.ParseTime(res.Expiry); err == nil {
				res.texpiry = t
			} else {
				util.Warnf("Unable to parse expires_at for %s: %v", res.Job.Jid, err)
			}
		}

		// Restore persisted extension so it survives restart.
		// Without this, an extended reservation would be reaped at its
		// original expiry after a restart even though the worker extended it.
		if !res.Extension.IsZero() && res.Extension.After(res.texpiry) {
			res.texpiry = res.Extension
			res.Expiry = util.Thens(res.Extension)
			// The sorted set score still reflects the original expiry.
			// Schedule a score update so the entry is found at the correct position.
			data, merr := json.Marshal(&res)
			if merr != nil {
				util.Warnf("Unable to marshal extended reservation for %s: %v", res.Job.Jid, merr)
			} else {
				updates = append(updates, scoreUpdate{entry: entry, newData: data, expiry: res.Expiry})
			}
		}

		m.workingMap[res.Job.Jid] = &res
		addedCount++
		return nil
	})

	if err != nil {
		util.Error("Error restoring working set", err)
		return fmt.Errorf("cannot restore working set: %w", err)
	}

	// Apply deferred sorted set score updates for extended reservations.
	// This is done outside the Each() iteration to avoid mutating the
	// sorted set while paging through it.
	for _, u := range updates {
		if err := m.store.Working().RemoveEntry(ctx, u.entry); err != nil {
			util.Warnf("Unable to update working set score: %v", err)
			continue
		}
		// Extract JID from the entry for AddElement
		var tmp Reservation
		if err := util.JsonUnmarshal(u.newData, &tmp); err != nil {
			util.Warnf("Unable to unmarshal for score update: %v", err)
			continue
		}
		if err := m.store.Working().AddElement(ctx, u.expiry, tmp.Job.Jid, u.newData); err != nil {
			util.Warnf("Unable to re-add extended reservation: %v", err)
		}
	}

	if addedCount > 0 {
		util.Debugf("Bootstrapped working set, loaded %d", addedCount)
	}

	return nil
}

func (m *manager) reserve(ctx context.Context, wid string, lease Lease) error {
	now := time.Now()
	job, _ := lease.Job()
	timeout := job.ReserveFor
	if timeout == 0 {
		timeout = DefaultTimeout
	}

	if timeout < 60 {
		util.Debugf("Timeout too short %d, 60 seconds minimum", timeout)
		timeout = 60
	}

	if timeout > 86400 {
		util.Debugf("Timeout too long %d, one day maximum", timeout)
		timeout = 86400
	}

	exp := now.Add(time.Duration(timeout) * time.Second)
	var res = &Reservation{
		lease:   lease,
		Job:     job,
		Since:   util.Thens(now),
		Expiry:  util.Thens(exp),
		Wid:     wid,
		tsince:  now,
		texpiry: exp,
	}

	data, err := json.Marshal(res)
	if err != nil {
		return fmt.Errorf("cannot marshal reservation payload: %w", err)
	}

	err = m.store.Working().AddElement(ctx, res.Expiry, job.Jid, data)
	if err != nil {
		return fmt.Errorf("cannot add element in the working set: %w", err)
	}

	m.workingMutex.Lock()
	m.workingMap[job.Jid] = res
	m.workingMutex.Unlock()

	return nil
}

func (m *manager) Acknowledge(ctx context.Context, jid string) (*client.Job, error) {
	res := m.clearReservation(jid)
	if res == nil {
		util.Infof("No such job to acknowledge %s", jid)
		return nil, nil
	}

	// doesn't matter, might not have acknowledged in time
	_, err := m.store.Working().RemoveElement(ctx, res.Expiry, jid)
	if err != nil {
		return nil, err
	}

	// Lease is in-memory only
	// A reservation can have a nil Lease if we restarted
	if res.lease != nil {
		err = res.lease.Release()
		if err != nil {
			util.Error("Error releasing lease for "+jid, err)
		}
	}

	if res.Job != nil {
		_ = m.store.Success(ctx)
		ctxh := context.WithValue(ctx, MiddlewareHelperKey, Ctx{res.Job, m, res})
		err = callMiddleware(ctxh, m.ackChain, func() error {
			return nil
		})
	}

	return res.Job, err
}

func (m *manager) ReapExpiredJobs(ctx context.Context, when time.Time) (int64, error) {
	total := int64(0)
	for {
		tm := util.Thens(when)
		count, err := m.store.Working().RemoveBefore(ctx, tm, 10, func(data []byte) error {
			var res Reservation
			err := util.JsonUnmarshal(data, &res)
			if err != nil {
				return fmt.Errorf("cannot unmarshal reservation payload: %w", err)
			}

			jid := res.Job.Jid
			m.workingMutex.Lock()
			localres, ok := m.workingMap[jid]
			m.workingMutex.Unlock()

			// the user has extended the job reservation.
			// Since modifying the score of a SortedSet member
			// is an expensive operation in Redis, we keep
			// the latest deadline in memory and extend the
			// reservation when it expires, in this method.
			if ok && when.Before(localres.extension) {
				localres.texpiry = localres.extension
				localres.Expiry = util.Thens(localres.extension)
				util.Debugf("Auto-extending reservation time for %s to %s", jid, localres.Expiry)
				// Re-marshal so the JSON member in the sorted set reflects
				// the updated Expiry (and any Extension) rather than the
				// stale original data that was just removed.
				newData, merr := json.Marshal(localres)
				if merr != nil {
					return fmt.Errorf("cannot marshal extended reservation for %q: %w", jid, merr)
				}
				err = m.store.Working().AddElement(ctx, localres.Expiry, jid, newData)
				if err != nil {
					return fmt.Errorf("cannot extend reservation for %q job: %w", jid, err)
				}
				return nil
			}

			job := res.Job
			err = m.processFailure(ctx, job.Jid, JobReservationExpired)
			if err != nil {
				return fmt.Errorf("cannot retry reservation: %w", err)
			}
			total += 1
			return nil
		})
		if err != nil {
			return total, err
		}
		if count < 10 {
			break
		}
	}

	return total, nil
}
