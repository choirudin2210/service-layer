package gocassa

import (
	"sync"
	"time"

	"github.com/bitly/go-hostpool"
	log "github.com/cihub/seelog"
	"github.com/gocql/gocql"
)

type gocqlConnCheckout struct {
	conn   *gocql.Conn
	hpResp hostpool.HostPoolResponse
}

// gocqlConnectionPool maintains a pool of connections to a list of hosts, selecting connections based on a linear
// epsilon greedy algorithm. It is not a "standard" gocql connection pool as it requires connections to be both
// checked-out and checked-in (to this end it has special coordination with gocqlExecutor).
//
// It does not have a global RWMutex as the config is considered immutable: after construction its host list is fixed,
// so locking is unnecessary. This makes the checkout/checkin path much faster.
type gocqlConnectionPool struct {
	Cfg            ksConfig
	hp             hostpool.HostPool
	activeConns    []*gocql.Conn
	activeConnsMtx sync.RWMutex

	hostConnsEstablished   map[string]chan *gocql.Conn
	hostConnsUnestablished map[string]chan *gocql.Conn
}

func (p *gocqlConnectionPool) init() {
	p.hp = hostpool.NewEpsilonGreedy(p.Cfg.hosts, 5*time.Minute, &hostpool.LinearEpsilonValueCalculator{})
	p.activeConns = make([]*gocql.Conn, 0, len(p.Cfg.hosts)*p.Cfg.cc.NumConns)

	p.hostConnsEstablished = make(map[string]chan *gocql.Conn, len(p.Cfg.hosts))
	p.hostConnsUnestablished = make(map[string]chan *gocql.Conn, len(p.Cfg.hosts))

	for _, host := range p.Cfg.hosts {
		p.hostConnsEstablished[host] = make(chan *gocql.Conn, p.Cfg.cc.NumConns)
		hostChanUn := make(chan *gocql.Conn, p.Cfg.cc.NumConns)
		for i := 0; i < cap(hostChanUn); i++ {
			hostChanUn <- nil
		}
		p.hostConnsUnestablished[host] = hostChanUn
	}
}

// checkout vends a connection checkout. If no connection could be checked-out, the conn of the resulting checkout will
// be nil (which gocql handles).
func (p *gocqlConnectionPool) checkout() gocqlConnCheckout {
	start := time.Now()
	hpResp := p.hp.Get()
	hostChanEstablished := p.hostConnsEstablished[hpResp.Host()]
	hostChanUnestablished := p.hostConnsUnestablished[hpResp.Host()]

	var conn *gocql.Conn
	select {
	case conn = <-hostChanEstablished:
	default:
		// We preferentially want to retrieve an already-established connection, hence why we have two nested selects
		// like this (a sort of "priority select")
		timeout := time.NewTimer(5 * time.Second)
		select {
		case conn = <-hostChanEstablished:
		case conn = <-hostChanUnestablished:
		case <-timeout.C:
			log.Warnf("[Cassandra:%s] Timed-out waiting for connection pool (took %s)", p.Cfg.ks, time.Since(start))
			return gocqlConnCheckout{
				conn:   nil,
				hpResp: hpResp,
			}
		}
		timeout.Stop()
	}

	newConn := false
	if conn == nil || conn.Closed() { // Establish the connection
		newConn = true
		conn = p.connect(hpResp.Host())
		if conn == nil {
			hostChanUnestablished <- conn // Be sure to put this back; it will not (cannot) be checked-in
		}
	}

	if conn != nil {
		if newConn {
			log.Tracef("[Cassandra:%s] Checked-out new connection to %s (took %s)", p.Cfg.ks, hpResp.Host(),
				time.Since(start).String())
		} else {
			log.Tracef("[Cassandra:%s] Checked-out reused connection to %s (took %s)", p.Cfg.ks, hpResp.Host(),
				time.Since(start).String())
		}
	} else {
		log.Tracef("[Cassandra:%s] Failed to check-out connection to %s (took %s)", p.Cfg.ks, hpResp.Host(),
			time.Since(start).String())
	}

	return gocqlConnCheckout{
		conn:   conn,
		hpResp: hpResp,
	}
}

func (p *gocqlConnectionPool) checkin(conn gocqlConnCheckout, err error) {
	conn.hpResp.Mark(err)
	if conn.conn != nil {
		p.hostConnsEstablished[conn.hpResp.Host()] <- conn.conn
		log.Tracef("[Cassandra:%s] Checked-in connection to %s", p.Cfg.ks, conn.hpResp.Host())
	}
}

func (p *gocqlConnectionPool) connect(host string) *gocql.Conn {
	cc := p.Cfg.cc
	connCfg := gocql.ConnConfig{
		ProtoVersion:  cc.ProtoVersion,
		CQLVersion:    cc.CQLVersion,
		Timeout:       cc.Timeout,
		NumStreams:    cc.NumStreams,
		Compressor:    cc.Compressor,
		Authenticator: cc.Authenticator,
		Keepalive:     cc.SocketKeepalive,
	}

	log.Tracef("[Cassandra:%s] Establishing new connection to %s…", p.Cfg.ks, host)
	if conn, err := gocql.Connect(host, connCfg, p); err != nil {
		log.Errorf("[Cassandra:%s] Error establishing connection to %s: %s", p.Cfg.ks, host, err.Error())
		return nil
	} else {
		p.activeConnsMtx.Lock()
		p.activeConns = append(p.activeConns, conn)
		p.activeConnsMtx.Unlock()
		return conn
	}
}

func (p *gocqlConnectionPool) Pick(*gocql.Query) *gocql.Conn {
	return nil // checkout() deals with this
}

func (p *gocqlConnectionPool) Size() int {
	p.activeConnsMtx.RLock()
	defer p.activeConnsMtx.RUnlock()
	return len(p.activeConns)
}

func (p *gocqlConnectionPool) HandleError(*gocql.Conn, error, bool) {
	// Handled by checkin() instead (because we need to report successes as well as failures)
}

func (p *gocqlConnectionPool) Close() {
	p.activeConnsMtx.Lock()
	defer p.activeConnsMtx.Unlock()
	for _, c := range p.activeConns { // The host channels will be handled by checkout()
		c.Close()
	}
	p.activeConns = make([]*gocql.Conn, 0, cap(p.activeConns))
}

func (p *gocqlConnectionPool) SetHosts(host []gocql.HostInfo) {
	// Config determines hosts: they are immutable
}
