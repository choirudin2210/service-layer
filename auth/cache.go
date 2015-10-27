package auth

import (
	"bytes"
	"time"

	log "github.com/cihub/seelog"

	"github.com/hailocab/gomemcache/memcache"
	inst "github.com/hailocab/service-layer/instrumentation"
	mc "github.com/hailocab/service-layer/memcache"
)

const (
	invalidPlaceholder = "invalid"
	invalidateTimeout  = 3600
)

// Cacher to store data
type Cacher interface {
	Store(u *User) error
	Invalidate(sessID string) error
	Fetch(sessID string) (u *User, cacheHit bool, err error)
	Purge(sessID string) error
}

type memcacheCacher struct{}

// Store will add a user to our token cache; non-nil error indicates we failed
// to add them to the token cache
func (c *memcacheCacher) Store(u *User) error {
	t := time.Now()
	err := c.doStore(u)
	instTiming("auth.cache.store", err, t)
	return err
}

func (c *memcacheCacher) doStore(u *User) error {
	ttl := int32(0)
	if !u.ExpiryTs.IsZero() {
		ttl = int32(u.ExpiryTs.Sub(time.Now()).Seconds())
	}
	return mc.Set(&memcache.Item{
		Key:        u.SessId,
		Value:      u.Token,
		Expiration: ttl,
	})
}

// Invalidate will keep track of the fact this sessID is not valid, to save
// us having to continually look it up with the login service; non-nil error
// indicates we failed to invalidate this in the cache
func (c *memcacheCacher) Invalidate(sessID string) error {
	t := time.Now()
	err := c.doInvalidate(sessID)
	instTiming("auth.cache.invalidate", err, t)
	return err
}

func (c *memcacheCacher) doInvalidate(sessID string) error {
	return mc.Set(&memcache.Item{
		Key:        sessID,
		Value:      []byte(invalidPlaceholder),
		Expiration: invalidateTimeout,
	})
}

// Fetch will attempt to retreive a user from token cache
// If cacheHit == true and u == nil and err == nil then we KNOW they don't
// exist (and so we don't have to bother looking them up via login service)
func (c *memcacheCacher) Fetch(sessID string) (u *User, cacheHit bool, err error) {
	t := time.Now()
	u, hit, err := c.doFetch(sessID)
	instTiming("auth.cache.fetch", err, t)
	if hit {
		inst.Counter(1.0, "auth.cache.fetch.hit", 1)
	} else {
		inst.Counter(1.0, "auth.cache.fetch.miss", 1)
	}
	return u, hit, err
}

func (c *memcacheCacher) doFetch(sessID string) (u *User, cacheHit bool, err error) {
	it, err := mc.Get(sessID)
	if err != nil && err != memcache.ErrCacheMiss {
		// actual error
		log.Warnf("[Auth] Token cache fetch error for '%s': %v", sessID, err)
		return nil, false, err
	}

	if err == memcache.ErrCacheMiss {
		// not found - not an error though
		log.Trace("[Auth] Token cache - miss")
		return nil, false, nil
	}

	if bytes.Equal(it.Value, []byte(invalidPlaceholder)) {
		// cached invalid
		log.Tracef("[Auth] Token cache - invalid placeholder in cache for %s", sessID)
		return nil, true, nil
	}

	u, err = FromSessionToken(sessID, string(it.Value))
	if err != nil {
		// found, but we can't decode - treat as not found
		log.Warnf("[Auth] Token cache decode error: %v", err)
		return nil, false, nil
	}

	return u, true, nil
}

// Purge will remove knowledge about a sessID from the token cache. If the
// sessID doesn't exist then this will be classed as success. Non-nil error
// indicates we failed to remove this cache key.
func (c *memcacheCacher) Purge(sessID string) error {
	t := time.Now()
	err := c.doPurge(sessID)
	instTiming("auth.cache.purge", err, t)

	return err
}

func (c *memcacheCacher) doPurge(sessID string) error {
	if err := mc.Delete(sessID); err != nil {
		return err
	}

	return nil
}
