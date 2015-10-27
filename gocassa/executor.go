package gocassa

import (
	"sync"
	"time"

	"errors"
	log "github.com/cihub/seelog"
	"github.com/gocql/gocql"
	"github.com/hailocab/gocassa"

	"github.com/hailocab/service-layer/config"
)

var (
	ksConnections    = map[string]gocassa.Connection{}
	ksConnectionsMtx sync.RWMutex
)

type gocqlExecutor struct {
	sync.RWMutex
	ks          string
	initialised bool
	initMtx     sync.RWMutex
	lastHash    uint32
	pool        *gocqlConnectionPool
}

func (e *gocqlExecutor) init() error {
	e.initMtx.RLock()
	if e.initialised {
		e.initMtx.RUnlock()
		return nil
	}

	e.initMtx.RUnlock()
	e.initMtx.Lock()
	defer e.initMtx.Unlock()
	if !e.initialised { // Guard against race
		cfg, err := getKsConfig(e.ks)
		if err != nil {
			return err
		}
		e.switchConfig(cfg)
		go e.watchConfig()
		e.initialised = true
	}
	return nil
}

func (e *gocqlExecutor) switchConfig(newConfig ksConfig) {
	e.Lock()
	defer e.Unlock()
	if e.pool != nil {
		e.pool.Close()
	}
	e.pool = &gocqlConnectionPool{
		Cfg: newConfig,
	}
	e.pool.init()
	e.lastHash = newConfig.hash()
	log.Infof("[Cassandra:%s] Switched config to: %s", e.ks, newConfig.String())
}

func (e *gocqlExecutor) watchConfig() {
	for _ = range config.SubscribeChanges() {
		e.RLock()
		ks := e.ks
		lastHash := e.lastHash
		e.RUnlock()
		if cfg, err := getKsConfig(ks); err != nil {
			log.Errorf("[Cassandra:%s] Error getting new config: %s", ks, err.Error())
		} else if cfg.hash() != lastHash {
			log.Infof("[Cassandra:%s] Config changed; invalidating connection pool", ks)
			e.switchConfig(cfg)
		} else {
			log.Debugf("[Cassandra:%s] Config changed but not invalidating connection pool (hash %d unchanged)", e.ks,
				e.lastHash)
		}
	}
}

func (e *gocqlExecutor) Query(stmt string, params ...interface{}) ([]map[string]interface{}, error) {
	if err := e.init(); err != nil {
		return nil, err
	}

	start := time.Now()
	e.RLock()
	pool, ks := e.pool, e.ks
	e.RUnlock()

	// Create a singlePool to vend an appropriate connection
	conn := pool.checkout()
	sp := singlePool{conn.conn}
	spCc := *(pool.Cfg.cc)
	spCc.ConnPoolType = sp.dummyNewPoolFunc() // Blergh
	sess, err := gocql.NewSession(spCc)
	if err != nil {
		pool.checkin(conn, err)
		return nil, err
	}
	sess.SetConsistency(pool.Cfg.cc.Consistency)

	iter := sess.Query(stmt, params...).Iter()
	results := []map[string]interface{}{}
	result := map[string]interface{}{}
	for iter.MapScan(result) {
		results = append(results, result)
		result = map[string]interface{}{}
	}
	err = iter.Close()
	log.Tracef("[Cassandra:%s] Query took %s: %s", ks, time.Since(start).String(), stmt)
	pool.checkin(conn, err)
	return results, err
}

func (e *gocqlExecutor) Execute(stmt string, params ...interface{}) error {
	if err := e.init(); err != nil {
		return err
	}

	start := time.Now()
	e.RLock()
	pool, ks := e.pool, e.ks
	e.RUnlock()

	// Create a singlePool to vend an appropriate connection
	conn := pool.checkout()
	sp := singlePool{conn.conn}
	spCc := *(pool.Cfg.cc)
	spCc.ConnPoolType = sp.dummyNewPoolFunc() // Blergh
	sess, err := gocql.NewSession(spCc)
	if err != nil {
		pool.checkin(conn, err)
		return err
	}
	sess.SetConsistency(pool.Cfg.cc.Consistency)

	err = sess.Query(stmt, params...).Exec()
	log.Tracef("[Cassandra:%s] Execute took %s: %s", ks, time.Since(start).String(), stmt)
	pool.checkin(conn, err)
	return err
}

func (e *gocqlExecutor) ExecuteAtomically(stmt []string, params [][]interface{}) error {
	return errors.New("Execute atomically is not implemented yet")
}

func gocqlConnector(ks string) gocassa.Connection {
	ksConnectionsMtx.RLock()
	conn, ok := ksConnections[ks]
	ksConnectionsMtx.RUnlock()
	if ok {
		return conn
	}

	ksConnectionsMtx.Lock()
	defer ksConnectionsMtx.Unlock()
	if conn, ok = ksConnections[ks]; !ok { // Guard against race
		conn = gocassa.NewConnection(&gocqlExecutor{
			ks: ks,
		})
		ksConnections[ks] = conn
	}
	return conn
}
