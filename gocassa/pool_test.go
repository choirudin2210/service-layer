package gocassa

import (
	"testing"
	"time"

	"github.com/gocql/gocql"

	ptesting "github.com/hailocab/platform-layer/testing"
)

type dummyRow struct {
	Id string
}

func TestPoolSuite(t *testing.T) {
	ptesting.RunSuite(t, new(poolSuite))
}

type poolSuite struct {
	ptesting.Suite
	pool *gocqlConnectionPool
}

func (suite *poolSuite) SetupSuite() {
	suite.Suite.SetupSuite()

	suite.pool = &gocqlConnectionPool{
		Cfg: ksConfig{
			ks:      "dummyks",
			hosts:   []string{"host1"},
			cl:      gocql.One,
			timeout: 1 * time.Second,
		},
	}
	cc := gocql.NewCluster(suite.pool.Cfg.hosts...)
	cc.Consistency = gocql.One
	cc.DiscoverHosts = false
	cc.NumConns = 2
	cc.Timeout = suite.pool.Cfg.timeout
	cc.Keyspace = suite.pool.Cfg.ks
	suite.pool.Cfg.cc = cc
	suite.pool.init()

	// Add a dummy connections to the pool (this won't work but we don't care for the purposes of these tests)
	host := suite.pool.Cfg.hosts[0]
	hostChan := suite.pool.hostConnsUnestablished[host]
	select {
	case <-hostChan:
		suite.pool.hostConnsEstablished[host] <- new(gocql.Conn)
	default:
		suite.Assertions.Fail("No unestablished connection could be dequeued")
	}
}

func (suite *poolSuite) TestConnectionReuse() {
	c := suite.pool.checkout()
	suite.Assertions.NotNil(c)
	suite.Assertions.NotNil(c.conn)
	suite.Assertions.NotNil(c.hpResp)
	conn, host := c.conn, c.hpResp.Host()
	suite.pool.checkin(c, nil)
	c = suite.pool.checkout()
	suite.Assertions.NotNil(c)
	suite.Assertions.Equal(host, c.hpResp.Host())
	suite.Assertions.Equal(conn, c.conn)
}
