package memcache

import (
	"fmt"
	"github.com/hailocab/service-layer/healthcheck"
	"github.com/hailocab/gomemcache/memcache"
)

const (
	HealthCheckId = "com.hailocab.service.memcache"
)

// HealthCheck asserts we can talk to memcache
func HealthCheck() healthcheck.Checker {
	return func() (map[string]string, error) {
		_, err := defaultClient.Get("healthcheck")
		if err != nil && err != memcache.ErrCacheMiss {
			return nil, fmt.Errorf("Memcache operation failed: %v", err)
		}

		return nil, nil
	}
}
