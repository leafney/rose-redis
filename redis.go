package rredis

import (
	"context"
	"crypto/tls"
	"errors"
	red "github.com/go-redis/redis/v8"
	"time"
)

type RType string

const (
	// TypeCluster means redis cluster.
	TypeCluster RType = "cluster"
	// TypeNode means redis node.
	TypeNode RType = "node"
	// Nil is an alias of redis.Nil.
	Nil = red.Nil

	blockingQueryTimeout = 5 * time.Second
	readWriteTimeout     = 2 * time.Second
	defaultSlowThreshold = time.Millisecond * 100

	defDatabase = 0
	maxRetries  = 3
	idleConns   = 8
)

type (
	Option struct {
		isCluster bool
		Type      RType
		Pwd       string
		Db        int
		Tls       bool
	}

	Redis struct {
		client red.UniversalClient
		ctx    context.Context
	}

	// GeoLocation is used with GeoAdd to add geospatial location.
	GeoLocation = red.GeoLocation
	// GeoRadiusQuery is used with GeoRadius to query geospatial index.
	GeoRadiusQuery = red.GeoRadiusQuery
	// GeoPos is used to represent a geo position.
	GeoPos = red.GeoPos

	// Pipeliner is an alias of redis.Pipeliner.
	Pipeliner = red.Pipeliner

	// Z represents sorted set member.
	Z = red.Z
	// ZStore is an alias of redis.ZStore.
	ZStore = red.ZStore

	// IntCmd is an alias of redis.IntCmd.
	IntCmd = red.IntCmd
	// FloatCmd is an alias of redis.FloatCmd.
	FloatCmd = red.FloatCmd
	// StringCmd is an alias of redis.StringCmd.
	StringCmd = red.StringCmd
)

func NewRedis(addr string, opt *Option) (*Redis, error) {
	rdc := loadOption(opt)
	r := new(Redis)

	if rdc.isCluster {
		var tlsConfig *tls.Config
		if rdc.Tls {
			tlsConfig = &tls.Config{
				InsecureSkipVerify: true,
			}
		}

		options := &red.ClusterOptions{
			Addrs:        splitClusterAddr(addr),
			Password:     rdc.Pwd,
			TLSConfig:    tlsConfig,
			MaxRetries:   maxRetries,
			MinIdleConns: idleConns,
		}

		client := red.NewClusterClient(options)
		r = &Redis{client: client, ctx: context.Background()}
	} else {
		options := &red.Options{
			Addr:         addr,
			Password:     rdc.Pwd,
			DB:           rdc.Db,
			MaxRetries:   maxRetries,
			MinIdleConns: idleConns,
		}
		client := red.NewClient(options)
		r = &Redis{client: client, ctx: context.Background()}
	}

	if !r.Ping() {
		return nil, errors.New("error Ping")
	}
	return r, nil
}

//func newRedis(addr string, opt *Option) *Option {
// Initial configuration
//r := &Redis{
//	Addr: addr,
//	Type: TypeNode,
//	Db:   defDatabase,
//	Pwd:  "",
//}
//
//// load user configuration
//for _, opt := range opts {
//	opt(r)
//}
//
//// is cluster mode
//r.isCluster = r.Type == TypeCluster
//
//return r

//}

func loadOption(opt *Option) *Option {
	o := &Option{
		Type:      TypeNode,
		Db:        defDatabase,
		isCluster: false,
		Pwd:       "",
		Tls:       false,
	}

	if opt == nil {
		return o
	}
	if opt.Db > 0 {
		o.Db = opt.Db
	}
	if len(opt.Pwd) > 0 {
		o.Pwd = opt.Pwd
	}
	o.isCluster = opt.Type == TypeCluster
	o.Tls = opt.Tls

	return o
}

// ------------------------

// Close
func (s *Redis) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

// Ping is the implementation of redis ping command.
func (s *Redis) Ping() bool {
	return s.PingCtx(s.ctx)
}

// PingCtx is the implementation of redis ping command.
func (s *Redis) PingCtx(ctx context.Context) (val bool) {
	v, err := s.client.Ping(ctx).Result()
	if err != nil {
		val = false
	}

	val = v == "PONG"
	return
}

// ------------------------

//
//// Blpop uses passed in redis connection to execute blocking queries.
//// Doesn't benefit from pooling redis connections of blocking queries
//func (s *Redis) Blpop(node RedisNode, key string) (string, error) {
//	return s.BlpopCtx(s.ctx, node, key)
//}
//
//// BlpopCtx uses passed in redis connection to execute blocking queries.
//// Doesn't benefit from pooling redis connections of blocking queries
//func (s *Redis) BlpopCtx(ctx context.Context, node RedisNode, key string) (string, error) {
//	return s.BlpopWithTimeoutCtx(ctx, node, blockingQueryTimeout, key)
//}
//
//// BlpopEx uses passed in redis connection to execute blpop command.
//// The difference against Blpop is that this method returns a bool to indicate success.
//func (s *Redis) BlpopEx(node RedisNode, key string) (string, bool, error) {
//	return s.BlpopExCtx(s.ctx, node, key)
//}
//
//// BlpopExCtx uses passed in redis connection to execute blpop command.
//// The difference against Blpop is that this method returns a bool to indicate success.
//func (s *Redis) BlpopExCtx(ctx context.Context, node RedisNode, key string) (string, bool, error) {
//	if node == nil {
//		return "", false, ErrNilNode
//	}
//
//	vals, err := node.BLPop(ctx, blockingQueryTimeout, key).Result()
//	if err != nil {
//		return "", false, err
//	}
//
//	if len(vals) < 2 {
//		return "", false, fmt.Errorf("no value on key: %s", key)
//	}
//
//	return vals[1], true, nil
//}
//
//
//// BlpopWithTimeout uses passed in redis connection to execute blpop command.
//// Control blocking query timeout
//func (s *Redis) BlpopWithTimeout(node RedisNode, timeout time.Duration, key string) (string, error) {
//	return s.BlpopWithTimeoutCtx(s.ctx, node, timeout, key)
//}
//
//// BlpopWithTimeoutCtx uses passed in redis connection to execute blpop command.
//// Control blocking query timeout
//func (s *Redis) BlpopWithTimeoutCtx(ctx context.Context, node RedisNode, timeout time.Duration,
//	key string) (string, error) {
//	if node == nil {
//		return "", ErrNilNode
//	}
//
//	vals, err := node.BLPop(ctx, timeout, key).Result()
//	if err != nil {
//		return "", err
//	}
//
//	if len(vals) < 2 {
//		return "", fmt.Errorf("no value on key: %s", key)
//	}
//
//	return vals[1], nil
//}

// ------------------------
