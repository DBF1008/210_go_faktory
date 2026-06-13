package server

import (
	"testing"
	"time"

	"github.com/contribsys/faktory/manager"
	"github.com/stretchr/testify/assert"
)

func TestServerOptionsDuration(t *testing.T) {
	def := 999 * time.Hour
	opts := &ServerOptions{GlobalConfig: map[string]any{
		"faktory": map[string]any{
			"dead_timeout":  "48h",
			"bad_duration":  "not-a-duration",
			"zero_duration": "0s",
			"neg_duration":  "-5m",
			"wrong_type":    int64(180),
		},
	}}

	// a valid Go duration string is parsed
	assert.Equal(t, 48*time.Hour, opts.Duration("faktory", "dead_timeout", def))

	// every invalid/missing case falls back to the supplied default
	assert.Equal(t, def, opts.Duration("missing", "dead_timeout", def))
	assert.Equal(t, def, opts.Duration("faktory", "missing", def))
	assert.Equal(t, def, opts.Duration("faktory", "bad_duration", def))
	assert.Equal(t, def, opts.Duration("faktory", "zero_duration", def))
	assert.Equal(t, def, opts.Duration("faktory", "neg_duration", def))
	assert.Equal(t, def, opts.Duration("faktory", "wrong_type", def))
}

// Verifies the HUP reload path: updating the global config and calling Reload
// must push the new dead retention into the manager so that subsequent jobs
// entering the morgue honor it.
func TestReloadUpdatesDeadTTL(t *testing.T) {
	runServer("localhost:4493", func(s *Server) {
		// no dead_timeout configured at boot -> the default applies
		assert.Equal(t, manager.DefaultDeadTTL, s.Manager().DeadTTL())

		s.Options.GlobalConfig = map[string]any{
			"faktory": map[string]any{"dead_timeout": "24h"},
		}
		s.Reload()
		assert.Equal(t, 24*time.Hour, s.Manager().DeadTTL())

		// an invalid value on reload reverts to the default
		s.Options.GlobalConfig = map[string]any{
			"faktory": map[string]any{"dead_timeout": "garbage"},
		}
		s.Reload()
		assert.Equal(t, manager.DefaultDeadTTL, s.Manager().DeadTTL())
	})
}
