package gocassa

import (
	"fmt"
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
	cfg         ksConfig
	session     *gocql.Session
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
		err = e.switchConfig(cfg)
		if err != nil {
			return err
		}
		go e.watchConfig()
		e.initialised = true
	}
	return nil
}

func (e *gocqlExecutor) switchConfig(newConfig ksConfig) error {
	e.Lock()
	defer e.Unlock()
	if e.session != nil {
		e.session.Close()
	}
	e.cfg = newConfig
	session, err := e.cfg.cc.CreateSession()
	if err != nil {
		return err
	}

	e.session = session
	e.lastHash = newConfig.hash()

	return nil
}

func (e *gocqlExecutor) watchConfig() {
	configCh := config.SubscribeChanges()
	retryCh := make(chan struct{})

	for {
		select {
		case <-configCh:
			e.reloadSession(retryCh)
		case <-retryCh:
			e.reloadSession(retryCh)
		}
	}
}

func (e *gocqlExecutor) reloadSession(retryCh chan struct{}) {
	e.RLock()
	ks := e.ks
	lastHash := e.lastHash
	e.RUnlock()

	if cfg, err := getKsConfig(ks); err != nil {
		log.Errorf("[Cassandra:%s] Error getting new config: %s", ks, err.Error())
	} else if cfg.hash() != lastHash {
		log.Infof("[Cassandra:%s] Config changed; invalidating connection pool", ks)

		if err := e.switchConfig(cfg); err != nil {
			log.Errorf("[Cassandra:%s] Error creating session, retrying after 1s delay: %s", ks, err)
			time.Sleep(time.Second)
			retryCh <- struct{}{}
		}

		log.Infof("[Cassandra:%s] Switched config to: %s", e.ks, cfg.String())
	} else {
		log.Debugf("[Cassandra:%s] Config changed but not invalidating connection pool (hash %d unchanged)", e.ks,
			e.lastHash)
	}
}

func (e *gocqlExecutor) Query(stmt string, params ...interface{}) ([]map[string]interface{}, error) {
	if err := e.init(); err != nil {
		return nil, err
	}

	start := time.Now()
	e.RLock()
	session, ks := e.session, e.ks
	e.RUnlock()

	if session == nil {
		return nil, fmt.Errorf("No open session")
	}

	iter := session.Query(stmt, params...).Iter()
	results := []map[string]interface{}{}
	result := map[string]interface{}{}
	for iter.MapScan(result) {
		results = append(results, result)
		result = map[string]interface{}{}
	}
	err := iter.Close()
	log.Tracef("[Cassandra:%s] Query took %s: %s", ks, time.Since(start).String(), stmt)
	return results, err
}

func (e *gocqlExecutor) Execute(stmt string, params ...interface{}) error {
	if err := e.init(); err != nil {
		return err
	}

	start := time.Now()
	e.RLock()
	session, ks := e.session, e.ks
	e.RUnlock()

	if session == nil {
		return fmt.Errorf("No open session")
	}

	err := session.Query(stmt, params...).Exec()
	log.Tracef("[Cassandra:%s] Execute took %s: %s", ks, time.Since(start).String(), stmt)
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
