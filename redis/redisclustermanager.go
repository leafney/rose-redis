package redis

import (
	"crypto/tls"
	"github.com/leafney/rose-redis"
	"strings"

	red "github.com/go-redis/redis/v8"
)

const addrSep = ","

func getCluster(r *rredis.Redis) (*red.ClusterClient, error) {
	//val, err := clusterManager.GetResource(r.Addr, func() (io.Closer, error) {
	//	var tlsConfig *tls.Config
	//	if r.tls {
	//		tlsConfig = &tls.Config{
	//			InsecureSkipVerify: true,
	//		}
	//	}
	//	store := red.NewClusterClient(&red.ClusterOptions{
	//		Addrs:        []r.addr,
	//		MaxRetries:   maxRetries,
	//		MinIdleConns: idleConns,
	//		TLSConfig:    tlsConfig,
	//	})
	//
	//	return store, nil
	//})

	var tlsConfig *tls.Config
	if r.Tls {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}
	store := red.NewClusterClient(&red.ClusterOptions{
		Addrs:        []string{r.Addr},
		Password:     r.Pass,
		MaxRetries:   maxRetries,
		MinIdleConns: idleConns,
		TLSConfig:    tlsConfig,
	})

	return store, nil

	//return val.(*red.ClusterClient), nil
}

func splitClusterAddrs(addr string) []string {
	addrs := strings.Split(addr, addrSep)
	unique := make(map[string]struct{})
	for _, each := range addrs {
		unique[strings.TrimSpace(each)] = struct{}{}
	}

	addrs = addrs[:0]
	for k := range unique {
		addrs = append(addrs, k)
	}

	return addrs
}
