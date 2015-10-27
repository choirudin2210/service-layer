package elasticsearch

import (
	"strconv"
	"sync"

	log "github.com/cihub/seelog"
	eapi "github.com/hailocab/elastigo/api"

	"github.com/hailocab/service-layer/config"
)

var (
	once sync.Once
)

func setup() {
	ch := config.SubscribeChanges()
	go func() {
		for {
			<-ch
			loadEndpointConfig()
		}
	}()

	loadEndpointConfig()
}

func loadEndpointConfig() {
	log.Info("Loading ElasticSearch config")

	port := config.AtPath("hailo", "service", "elasticsearch", "port").AsInt(9200)
	hosts := config.AtPath("hailo", "service", "elasticsearch", "hosts").AsHostnameArray(port)

	if len(hosts) == 0 {
		hosts = append(hosts, "localhost:19200")
	}

	// Set these hosts in the Elasticsearch library
	// This will initialise a host pool which uses an Epsilon Greedy algorithm to find healthy hosts
	// and send to requests to them, and not unhealthy or slow hosts
	eapi.Port = strconv.Itoa(port)
	if port == 443 {
		eapi.Protocol = "https"
	}
	eapi.SetHosts(hosts)

	log.Infof("ElasticSearch hosts loaded: %v", eapi.Hosts)
}

// LoadConfig gets the configuration from the Config Service and modifies elastigo variables with those values.
// setup method gets executed once and after that, there is a goroutine subscribed to any changes in the config service
func LoadConfig() {
	once.Do(setup)
}
