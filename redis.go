package rredis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	red "github.com/redis/go-redis/v9"
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

var (
	// ErrNilNode is an error that indicates a nil redis node.
	ErrNilNode = errors.New("nil redis node")
)

type (
	// A Pair is a key/pair set used in redis zset.
	Pair struct {
		Key   string
		Score int64
	}

	// A FloatPair is a key/pair for float set used in redis zet.
	FloatPair struct {
		Key   string
		Score float64
	}

	Option struct {
		isCluster bool
		//
		Type RType
		Pass string
		DB   int
		Tls  bool
	}

	Redis struct {
		client red.UniversalClient
		ctx    context.Context
	}

	// RedisNode interface represents a redis node.
	RedisNode interface {
		red.Cmdable
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

func NewClient(addr string, opt *Option) *Redis {
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
			Password:     rdc.Pass,
			TLSConfig:    tlsConfig,
			MaxRetries:   maxRetries,
			MinIdleConns: idleConns,
		}

		client := red.NewClusterClient(options)
		r = &Redis{client: client, ctx: context.Background()}
	} else {
		options := &red.Options{
			Addr:         addr,
			Password:     rdc.Pass,
			DB:           rdc.DB,
			MaxRetries:   maxRetries,
			MinIdleConns: idleConns,
		}
		client := red.NewClient(options)
		r = &Redis{client: client, ctx: context.Background()}
	}

	return r
}

func NewRedis(addr string, opt *Option) (*Redis, error) {
	r := NewClient(addr, opt)
	if !r.Ping() {
		return nil, errors.New("error Ping")
	}
	return r, nil
}

// MustNewRedis returns a Redis with given options.
func MustNewRedis(addr string, opts *Option) *Redis {
	rds, err := NewRedis(addr, opts)
	if err != nil {
		msg := fmt.Sprintf("%+v\n\n", err.Error())
		panic(msg)
	}

	return rds
}

//func newRedis(addr string, opt *Option) *Option {
// Initial configuration
//r := &Redis{
//	Addr: addr,
//	Type: TypeNode,
//	DB:   defDatabase,
//	Pass:  "",
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
		DB:        defDatabase,
		isCluster: false,
		Pass:      "",
		Tls:       false,
	}

	if opt == nil {
		return o
	}
	if opt.DB > 0 {
		o.DB = opt.DB
	}
	if len(opt.Pass) > 0 {
		o.Pass = opt.Pass
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
