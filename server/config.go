package server

import (
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

// Duration reads a Go duration string (e.g. "4320h") from the named config
// element and parses it with time.ParseDuration. If the value is missing,
// not a string, unparseable, or non-positive, defval is returned and a
// warning is logged.
func (so *ServerOptions) Duration(subsys string, key string, defval time.Duration) time.Duration {
	val := so.Config(subsys, key, nil)
	if val == nil {
		return defval
	}
	str, ok := val.(string)
	if !ok {
		util.Warnf("Config error: %s/%s must be a duration string, using default %v", subsys, key, defval)
		return defval
	}
	dur, err := time.ParseDuration(str)
	if err != nil {
		util.Warnf("Config error: %s/%s is not a valid duration (%q): %v, using default %v", subsys, key, str, err, defval)
		return defval
	}
	if dur <= 0 {
		util.Warnf("Config error: %s/%s must be a positive duration, using default %v", subsys, key, defval)
		return defval
	}
	return dur
}
