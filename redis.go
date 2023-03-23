package rredis

import (
	"context"
	"crypto/tls"
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

	defaultDatabase = 0
	maxRetries      = 3
	idleConns       = 8
)

type (
	Option func(r *Redis)

	Redis struct {
		client    red.UniversalClient
		isCluster bool
		Addr      string
		Type      string
		Pass      string
		Db        int
		Tls       bool
	}
)

func NewRedis(addr string, opts ...Option) (*Redis, error) {
	rdc := newRedis(addr, opts...)

	r := new(Redis)

	if rdc.isCluster {
		var tlsConfig *tls.Config
		if rdc.Tls {
			tlsConfig = &tls.Config{
				InsecureSkipVerify: true,
			}
		}

		options := &red.ClusterOptions{
			Addrs:        splitClusterAddr(rdc.Addr),
			Password:     rdc.Pass,
			TLSConfig:    tlsConfig,
			MaxRetries:   maxRetries,
			MinIdleConns: idleConns,
		}

		client := red.NewClusterClient(options)
		r = &Redis{client: client}
	} else {
		options := &red.Options{
			Addr:         rdc.Addr,
			Password:     rdc.Pass,
			DB:           rdc.Db,
			MaxRetries:   maxRetries,
			MinIdleConns: idleConns,
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
		Db:   defaultDatabase,
		Pass: "",
	}

	// 载入用户配置
	for _, opt := range opts {
		opt(r)
	}

	// 是否集群判断
	r.isCluster = r.Type == ClusterType

	return r
}

// ------------------------

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

// ------------------------

// BitCount is redis bitcount command implementation.
func (s *Redis) BitCount(key string, start, end int64) (int64, error) {
	return s.BitCountCtx(context.Background(), key, start, end)
}

// BitCountCtx is redis bitcount command implementation.
func (s *Redis) BitCountCtx(ctx context.Context, key string, start, end int64) (val int64, err error) {
	return s.client.BitCount(ctx, key, &red.BitCount{
		Start: start,
		End:   end,
	}).Result()
}

// BitOpAnd is redis bit operation (and) command implementation.
func (s *Redis) BitOpAnd(destKey string, keys ...string) (int64, error) {
	return s.BitOpAndCtx(context.Background(), destKey, keys...)
}

// BitOpAndCtx is redis bit operation (and) command implementation.
func (s *Redis) BitOpAndCtx(ctx context.Context, destKey string, keys ...string) (val int64, err error) {
	return s.client.BitOpAnd(ctx, destKey, keys...).Result()
}

// BitOpNot is redis bit operation (not) command implementation.
func (s *Redis) BitOpNot(destKey, key string) (int64, error) {
	return s.BitOpNotCtx(context.Background(), destKey, key)
}

// BitOpNotCtx is redis bit operation (not) command implementation.
func (s *Redis) BitOpNotCtx(ctx context.Context, destKey, key string) (val int64, err error) {
	return s.client.BitOpNot(ctx, destKey, key).Result()
}

// BitOpOr is redis bit operation (or) command implementation.
func (s *Redis) BitOpOr(destKey string, keys ...string) (int64, error) {
	return s.BitOpOrCtx(context.Background(), destKey, keys...)
}

// BitOpOrCtx is redis bit operation (or) command implementation.
func (s *Redis) BitOpOrCtx(ctx context.Context, destKey string, keys ...string) (val int64, err error) {
	return s.client.BitOpOr(ctx, destKey, keys...).Result()
}

// BitOpXor is redis bit operation (xor) command implementation.
func (s *Redis) BitOpXor(destKey string, keys ...string) (int64, error) {
	return s.BitOpXorCtx(context.Background(), destKey, keys...)
}

// BitOpXorCtx is redis bit operation (xor) command implementation.
func (s *Redis) BitOpXorCtx(ctx context.Context, destKey string, keys ...string) (val int64, err error) {
	return s.client.BitOpXor(ctx, destKey, keys...).Result()
}

// BitPos is redis bitpos command implementation.
func (s *Redis) BitPos(key string, bit, start, end int64) (int64, error) {
	return s.BitPosCtx(context.Background(), key, bit, start, end)
}

// BitPosCtx is redis bitpos command implementation.
func (s *Redis) BitPosCtx(ctx context.Context, key string, bit, start, end int64) (val int64, err error) {
	return s.client.BitPos(ctx, key, bit, start, end).Result()
}

// ------------------------

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
