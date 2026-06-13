package webui

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestLiveServer(t *testing.T) {
	bootRuntime(t, "webui", func(ui *WebUI, s *server.Server, t *testing.T) {
		t.Run("StaticAssets", func(t *testing.T) {
			req, err := ui.NewRequest("GET", "http://localhost:7420/static/application.js", nil)
			assert.NoError(t, err)

			w := httptest.NewRecorder()
			staticHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), "ready"), w.Body.String())
		})

		t.Run("Debug", func(t *testing.T) {
			req, err := ui.NewRequest("GET", "http://localhost:7420/debug", nil)
			assert.NoError(t, err)

			w := httptest.NewRecorder()
			debugHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), "Disk Usage"), w.Body.String())
		})

		t.Run("Health", func(t *testing.T) {
			req, err := ui.NewRequest("GET", "http://localhost:7420/health", nil)
			assert.NoError(t, err)

			w := httptest.NewRecorder()
			healthHandler(ui)(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), "faktory_version"), w.Body.String())
		})

		t.Run("ComputeLocale", func(t *testing.T) {
			lang := localeFromHeader("")
			assert.Equal(t, "en", lang)
			lang = localeFromHeader(" 'fr-FR,fr;q=0.8,en-US;q=0.6,en;q=0.4,ru;q=0.2'")
			assert.Equal(t, "fr", lang)
			lang = localeFromHeader("zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4,ru;q=0.2")
			assert.Equal(t, "zh-cn", lang)
			lang = localeFromHeader("en-US,sv-SE;q=0.8,sv;q=0.6,en;q=0.4")
			assert.Equal(t, "sv", lang)
			lang = localeFromHeader("nb-NO,nb;q=0.2")
			assert.Equal(t, "nb", lang)
			lang = localeFromHeader("en-us")
			assert.Equal(t, "en", lang)
			lang = localeFromHeader("sv-se")
			assert.Equal(t, "sv", lang)
			lang = localeFromHeader("pt-BR,pt;q=0.8,en-US;q=0.6,en;q=0.4")
			assert.Equal(t, "pt-br", lang)
			lang = localeFromHeader("pt-PT,pt;q=0.8,en-US;q=0.6,en;q=0.4")
			assert.Equal(t, "pt", lang)
			lang = localeFromHeader("pt-br")
			assert.Equal(t, "pt-br", lang)
			lang = localeFromHeader("pt-pt")
			assert.Equal(t, "pt", lang)
			lang = localeFromHeader("pt")
			assert.Equal(t, "pt", lang)
			lang = localeFromHeader("en-us; *")
			assert.Equal(t, "en", lang)
			lang = localeFromHeader("en-US,en;q=0.8")
			assert.Equal(t, "en", lang)
			lang = localeFromHeader("en-GB,en-US;q=0.8,en;q=0.6")
			assert.Equal(t, "en", lang)
			lang = localeFromHeader("ru,en")
			assert.Equal(t, "ru", lang)
			lang = localeFromHeader("*")
			assert.Equal(t, "en", lang)
		})
	})
}

func bootRuntime(t *testing.T, name string, fn func(*WebUI, *server.Server, *testing.T)) {
	dir := fmt.Sprintf("/tmp/faktory-test-%s", name)
	defer os.RemoveAll(dir)

	sock := fmt.Sprintf("%s/redis.sock", dir)
	stopper, err := storage.Boot(dir, sock)
	if err != nil {
		panic(err)
	}
	defer func() { _ = stopper() }()

	s, err := server.NewServer(&server.ServerOptions{
		Binding:          "localhost:7418",
		StorageDirectory: dir,
		RedisSock:        sock,
		PoolSize:         server.DefaultMaxPoolSize,
	})

	if err != nil {
		panic(err)
	}
	err = s.Boot()
	if err != nil {
		panic(err)
	}
	defer s.Stop(nil)
	assert.NoError(t, s.Store().Flush(context.Background()))

	go func() {
		err := s.Run()
		if err != nil {
			panic(err)
		}
	}()

	web := newWeb(s, defaultOptions())

	fn(web, s, t)
}

func fakeJob() (string, []byte) {
	jid := util.RandomJid()
	nows := util.Nows()
	return jid, fmt.Appendf(nil, `{
		"jid":"%s",
		"created_at":"%s",
		"queue":"default",
		"args":[1,2,3],
		"jobtype":"SomeWorker",
		"at":"%s",
		"enqueued_at":"%s",
		"failure":{
		"retry_count":0,
		"failed_at":"%s",
		"message":"Invalid argument",
			"errtype":"RuntimeError"
		},
		"custom":{
			"foo":"bar",
			"tenant":1
		}
	}`, jid, nows, nows, nows, nows)
}

// TestKillUsesConfiguredDeadTTL verifies the Web UI "kill" action stamps dead
// jobs with the manager's configured retention rather than a hardcoded TTL,
// keeping the Web UI consistent with the fail and MUTATE KILL paths.
func TestKillUsesConfiguredDeadTTL(t *testing.T) {
	bootRuntime(t, "killttl", func(ui *WebUI, s *server.Server, t *testing.T) {
		bg := context.Background()
		assert.NoError(t, s.Store().Flush(bg))

		// an operator-configured retention, distinct from the default
		ttl := 96 * time.Hour
		s.Manager().SetDeadTTL(ttl)

		retries := s.Store().Retries()
		jid, data := fakeJob()
		assert.NoError(t, retries.AddElement(bg, util.Nows(), jid, data))

		var key []byte
		assert.NoError(t, retries.Each(bg, func(idx int, e storage.SortedEntry) error {
			var err error
			key, err = e.Key()
			return err
		}))

		// drive the same code path the Scheduled/Retries pages use for "kill"
		req, err := ui.NewRequest("POST", "http://localhost:7420/retries", nil)
		assert.NoError(t, err)

		before := time.Now()
		assert.NoError(t, actOn(req, retries, "kill", []string{string(key)}))
		after := time.Now()

		assert.EqualValues(t, 0, retries.Size(bg))
		assert.EqualValues(t, 1, s.Store().Dead().Size(bg))

		expiry := firstDeadExpiry(t, bg, s.Store())
		assert.WithinRange(t, expiry,
			before.Add(ttl).Add(-2*time.Second),
			after.Add(ttl).Add(2*time.Second))
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
