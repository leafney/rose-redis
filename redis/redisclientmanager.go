package redis

import (
	"crypto/tls"
	red "github.com/go-redis/redis/v8"
	"github.com/leafney/rose-redis"
)

const (
	defaultDatabase = 0
	maxRetries      = 3
	idleConns       = 8
)

//var clientManager = syncx.NewResourceManager()

func getClient(r *rredis.Redis) (*red.Client, error) {
	var tlsConfig *tls.Config
	if r.Tls {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}
	store := red.NewClient(&red.Options{
		Addr:         r.Addr,
		Password:     r.Pass,
		DB:           r.Db,
		MaxRetries:   maxRetries,
		MinIdleConns: idleConns,
		TLSConfig:    tlsConfig,
	})

	return store, nil

	//return store.(*red.Client), nil
}
