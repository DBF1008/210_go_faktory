package server

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/contribsys/faktory/util"
)

// This is the ultimate scalability limitation in Faktory,
// we only allow this many connections to Redis.
var DefaultMaxPoolSize uint64 = 2000

type ServerOptions struct {
	GlobalConfig     map[string]any
	Binding          string
	StorageDirectory string
	RedisSock        string
	ConfigDirectory  string
	Environment      string
	Password         string //gosec:disable
	PoolSize         uint64
}

func (so *ServerOptions) String(subsys string, key string, defval string) string {
	val := so.Config(subsys, key, defval)
	str, ok := val.(string)
	if !ok {
		util.Warnf("Config error: %s/%s is not a String", subsys, key)
		return defval
	}
	return str
}

func (so *ServerOptions) Config(subsys string, key string, defval any) any {
	mapp, ok := so.GlobalConfig[subsys]
	if !ok {
		return defval
	}

	maps, ok := mapp.(map[string]any)
	if !ok {
		util.Warnf("Invalid configuration, expected a %s subsystem, using default", subsys)
		return defval
	}

	val, ok := maps[key]
	if !ok {
		return defval
	}
	return val
}

// Duration retrieves a configuration value as a time.Duration.
// It accepts:
//   - Go duration strings (e.g. "2160h", "7776000s", "130m")
//   - Day-based strings (e.g. "180d", "90d")
//   - Raw time.Duration values (from TOML unmarshalling)
//   - Integer values (interpreted as seconds)
func (so *ServerOptions) Duration(subsys string, key string, defval time.Duration) time.Duration {
	val := so.Config(subsys, key, nil)
	if val == nil {
		return defval
	}
	switch v := val.(type) {
	case time.Duration:
		return v
	case string:
		return parseDuration(v, defval)
	case int64:
		return time.Duration(v) * time.Second
	case float64:
		return time.Duration(v) * time.Second
	default:
		util.Warnf("Config error: %s/%s is not a recognized duration type (%T), using default", subsys, key, val)
		return defval
	}
}

// parseDuration parses a duration string supporting both Go standard
// durations and a "Nd" suffix for days (e.g. "90d" = 90 days).
func parseDuration(s string, defval time.Duration) time.Duration {
	s = strings.TrimSpace(s)
	if s == "" {
		return defval
	}
	if strings.HasSuffix(s, "d") {
		numStr := strings.TrimSuffix(s, "d")
		if days, err := strconv.Atoi(numStr); err == nil {
			return time.Duration(days) * 24 * time.Hour
		}
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		util.Warnf("Config error: cannot parse duration %q: %v, using default", s, err)
		return defval
	}
	return d
}

// FormatDuration formats a time.Duration as a human-readable string,
// using days when the duration is an exact multiple of 24 hours.
func FormatDuration(d time.Duration) string {
	if d >= 24*time.Hour && d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
	return d.String()
}
