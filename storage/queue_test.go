package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestBasicQueueOps(t *testing.T) {
	withRedis(t, "queue", func(t *testing.T, store Store) {
		bg := context.Background()

		t.Run("Push", func(t *testing.T) {
			_ = store.Flush(bg)
			q, err := store.GetQueue(bg, "default")
			assert.NoError(t, err)

			assert.EqualValues(t, 0, q.Size(bg))

			data, err := q.Pop(bg)
			assert.NoError(t, err)
			assert.Nil(t, data)

			err = q.Push(bg, []byte("hello"))
			assert.NoError(t, err)
			assert.EqualValues(t, 1, q.Size(bg))

			err = q.Push(bg, []byte("world"))
			assert.NoError(t, err)
			assert.EqualValues(t, 2, q.Size(bg))

			values := [][]byte{
				[]byte("world"),
				[]byte("hello"),
			}
			err = q.Each(bg, func(idx int, value []byte) error {
				assert.Equal(t, values[idx], value)
				return nil
			})
			assert.NoError(t, err)

			data, err = q.Pop(bg)
			assert.NoError(t, err)
			assert.Equal(t, []byte("hello"), data)
			assert.EqualValues(t, 1, q.Size(bg))

			cnt, err := q.Clear(bg)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, cnt)
			assert.EqualValues(t, 0, q.Size(bg))

			// valid names:
			_, err = store.GetQueue(bg, "A-Za-z0-9_.-")
			assert.NoError(t, err)
			_, err = store.GetQueue(bg, "-")
			assert.NoError(t, err)
			_, err = store.GetQueue(bg, "A")
			assert.NoError(t, err)
			_, err = store.GetQueue(bg, "a")
			assert.NoError(t, err)

			// invalid names:
			_, err = store.GetQueue(bg, "default?page=1")
			assert.Error(t, err)
			_, err = store.GetQueue(bg, "user@example.com")
			assert.Error(t, err)
			_, err = store.GetQueue(bg, "c&c")
			assert.Error(t, err)
			_, err = store.GetQueue(bg, "priority|high")
			assert.Error(t, err)
			_, err = store.GetQueue(bg, "")
			assert.Error(t, err)
		})

		t.Run("heavy", func(t *testing.T) {
			_ = store.Flush(bg)
			q, err := store.GetQueue(bg, "default")
			assert.NoError(t, err)

			assert.EqualValues(t, 0, q.Size(bg))
			err = q.Push(bg, []byte("first"))
			assert.NoError(t, err)
			n := 5000
			// Push N jobs to queue
			// Get Size() each time
			for i := range n {
				_, data := fakeJob()
				err = q.Push(bg, data)
				assert.NoError(t, err)
				assert.EqualValues(t, i+2, q.Size(bg))
			}

			err = q.Push(bg, []byte("last"))
			assert.NoError(t, err)
			assert.EqualValues(t, n+2, q.Size(bg))

			q, err = store.GetQueue(bg, "default")
			assert.NoError(t, err)

			// Pop N jobs from queue
			// Get Size() each time
			assert.EqualValues(t, n+2, q.Size(bg))
			data, err := q.Pop(bg)
			assert.NoError(t, err)
			assert.Equal(t, []byte("first"), data)
			for i := range n {
				_, err := q.Pop(bg)
				assert.NoError(t, err)
				assert.EqualValues(t, n-i, q.Size(bg))
			}
			data, err = q.Pop(bg)
			assert.NoError(t, err)
			assert.Equal(t, []byte("last"), data)
			assert.EqualValues(t, 0, q.Size(bg))

			data, err = q.Pop(bg)
			assert.NoError(t, err)
			assert.Nil(t, data)
		})

		t.Run("threaded", func(t *testing.T) {
			_ = store.Flush(bg)
			q, err := store.GetQueue(bg, "default")
			assert.NoError(t, err)

			tcnt := 5
			n := 1000

			var wg sync.WaitGroup
			for range tcnt {
				wg.Go(func() {
					pushAndPop(t, n, q)
				})
			}

			wg.Wait()
			assert.EqualValues(t, 0, counter)
			assert.EqualValues(t, 0, q.Size(bg))

			err = q.Each(bg, func(idx int, v []byte) error {
				atomic.AddInt64(&counter, 1)
				// log.Println(string(k), string(v))
				return nil
			})
			assert.NoError(t, err)
			assert.EqualValues(t, 0, counter)
		})

		t.Run("PushBulk", func(t *testing.T) {
			_ = store.Flush(bg)
			q, err := store.GetQueue(bg, "bulk")
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))

			// Empty bulk push
			err = q.PushBulk(bg, [][]byte{})
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))

			// Push multiple items
			payloads := [][]byte{
				[]byte(`{"jid":"job1","queue":"bulk"}`),
				[]byte(`{"jid":"job2","queue":"bulk"}`),
				[]byte(`{"jid":"job3","queue":"bulk"}`),
			}
			err = q.PushBulk(bg, payloads)
			assert.NoError(t, err)
			assert.EqualValues(t, 3, q.Size(bg))

			// Verify items are present
			count := 0
			err = q.Each(bg, func(idx int, data []byte) error {
				count++
				return nil
			})
			assert.NoError(t, err)
			assert.Equal(t, 3, count)

			// Bulk push to different queue
			q2, err := store.GetQueue(bg, "bulk2")
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q2.Size(bg))

			payloads2 := [][]byte{
				[]byte(`{"jid":"job4"}`),
				[]byte(`{"jid":"job5"}`),
			}
			err = q2.PushBulk(bg, payloads2)
			assert.NoError(t, err)
			assert.EqualValues(t, 2, q2.Size(bg))

			// Original queue unchanged
			assert.EqualValues(t, 3, q.Size(bg))
		})
	})
}

var (
	counter int64
)

func pushAndPop(t *testing.T, n int, q Queue) {
	bg := context.Background()
	for range n {
		_, data := fakeJob()
		err := q.Push(bg, data)
		assert.NoError(t, err)
		atomic.AddInt64(&counter, 1)
	}

	for range n {
		value, err := q.Pop(bg)
		assert.NoError(t, err)
		assert.NotNil(t, value)
		atomic.AddInt64(&counter, -1)
	}
}

func fakeJob() (string, []byte) {
	jid := util.RandomJid()
	nows := util.Nows()
	return jid, fmt.Appendf(nil, `{"jid":%q,"created_at":%q,"queue":"default","args":[1,2,3],"class":"SomeWorker"}`, jid, nows)
}
