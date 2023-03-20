package redis

import (
	"context"
	"errors"
	red "github.com/go-redis/redis/v8"
	"time"
)

const (
	// ClusterType means redis cluster.
	ClusterType = "cluster"
	// NodeType means redis node.
	NodeType = "node"
	// Nil is an alias of redis.Nil.
	Nil = red.Nil

	blockingQueryTimeout = 5 * time.Second
	readWriteTimeout     = 2 * time.Second
	defaultSlowThreshold = time.Millisecond * 100
)

type (
	Option func(r *Redis)

	Redis struct {
		client    red.UniversalClient
		Addr      string
		Type      string
		Pass      string
		Db        int
		Tls       bool
		isCluster bool
	}

	//// RedisNode interface represents a redis node.
	//RedisNode interface {
	//	red.Cmdable
	//}
)

func NewRedis(addr string, opts ...Option) (*Redis, error) {
	rdc := newRedis(addr, opts...)

	r := new(Redis)

	if rdc.isCluster {
		options := &red.ClusterOptions{
			Addrs:    []string{rdc.Addr},
			Password: rdc.Pass,
		}
		client := red.NewClusterClient(options)
		r = &Redis{client: client}
	} else {
		options := &red.Options{
			Addr:     rdc.Addr,
			Password: rdc.Pass,
			DB:       rdc.Db,
		}
		client := red.NewClient(options)
		r = &Redis{client: client}
	}

	if !r.Ping() {
		return nil, errors.New("error Ping")
	}
	return r, nil
}

func newRedis(addr string, opts ...Option) *Redis {
	// 初始化默认配置
	r := &Redis{
		Addr: addr,
		Type: NodeType,
	}

	// 载入用户配置
	for _, opt := range opts {
		opt(r)
	}

	// 是否集群判断
	r.isCluster = r.Type == ClusterType

	return r
}

//func getRedis(r *Redis) (RedisNode, error) {
//	switch r.Type {
//	case ClusterType:
//		return getCluster(r)
//	case NodeType:
//		return getClient(r)
//	default:
//		return nil, fmt.Errorf("redis type '%s' is not supported", r.Type)
//	}
//}

// Ping is the implementation of redis ping command.
func (s *Redis) Ping() bool {
	return s.PingCtx(context.Background())
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

// Set is the implementation of redis set command.
func (s *Redis) Set(key, value string) error {
	return s.SetCtx(context.Background(), key, value)
}

// SetCtx is the implementation of redis set command.
func (s *Redis) SetCtx(ctx context.Context, key, value string) error {
	return s.client.Set(ctx, key, value, 0).Err()
}

// Get is the implementation of redis get command.
func (s *Redis) Get(key string) (string, error) {
	return s.GetCtx(context.Background(), key)
}

// GetCtx is the implementation of redis get command.
func (s *Redis) GetCtx(ctx context.Context, key string) (val string, err error) {
	if val, err = s.client.Get(ctx, key).Result(); err == red.Nil {
		return val, nil
	} else if err != nil {
		return "", err
	} else {
		return val, nil
	}
}
