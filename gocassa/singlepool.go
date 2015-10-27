package gocassa

import (
	"github.com/gocql/gocql"
)

// singlePool is a dummy gocql ConnectionPool that only yields a single connection. It's crufty.
type singlePool struct {
	*gocql.Conn
}

func (p singlePool) Pick(q *gocql.Query) *gocql.Conn {
	return p.Conn
}

func (p singlePool) Size() int {
	return 1
}

func (p singlePool) HandleError(conn *gocql.Conn, err error, closed bool) {
}

func (p singlePool) Close() {
	p.Conn.Close()
}

func (p singlePool) SetHosts(host []gocql.HostInfo) {

}

func (p singlePool) dummyNewPoolFunc() gocql.NewPoolFunc {
	return func(*gocql.ClusterConfig) (gocql.ConnectionPool, error) {
		return p, nil
	}
}
