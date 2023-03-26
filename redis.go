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
	// Initial configuration
	r := &Redis{
		Addr: addr,
		Type: NodeType,
		Db:   defaultDatabase,
		Pass: "",
	}

	// load user configuration
	for _, opt := range opts {
		opt(r)
	}

	// is cluster mode
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

//
//// Blpop uses passed in redis connection to execute blocking queries.
//// Doesn't benefit from pooling redis connections of blocking queries
//func (s *Redis) Blpop(node RedisNode, key string) (string, error) {
//	return s.BlpopCtx(context.Background(), node, key)
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
//	return s.BlpopExCtx(context.Background(), node, key)
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
//	return s.BlpopWithTimeoutCtx(context.Background(), node, timeout, key)
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

// Decr is the implementation of redis decr command.
func (s *Redis) Decr(key string) (int64, error) {
	return s.DecrCtx(context.Background(), key)
}

// DecrCtx is the implementation of redis decr command.
func (s *Redis) DecrCtx(ctx context.Context, key string) (val int64, err error) {
	return s.client.Decr(ctx, key).Result()
}

// DecrBy is the implementation of redis decrby command.
func (s *Redis) DecrBy(key string, decrement int64) (int64, error) {
	return s.DecrByCtx(context.Background(), key, decrement)
}

// DecrByCtx is the implementation of redis decrby command.
func (s *Redis) DecrByCtx(ctx context.Context, key string, decrement int64) (val int64, err error) {
	return s.client.DecrBy(ctx, key, decrement).Result()
}

// Del deletes keys.
func (s *Redis) Del(keys ...string) (int64, error) {
	return s.DelCtx(context.Background(), keys...)
}

// DelCtx deletes keys.
func (s *Redis) DelCtx(ctx context.Context, keys ...string) (val int64, err error) {
	return s.client.Del(ctx, keys...).Result()
}

// Eval is the implementation of redis eval command.
func (s *Redis) Eval(script string, keys []string, args ...interface{}) (interface{}, error) {
	return s.EvalCtx(context.Background(), script, keys, args...)
}

// EvalCtx is the implementation of redis eval command.
func (s *Redis) EvalCtx(ctx context.Context, script string, keys []string,
	args ...interface{}) (val interface{}, err error) {
	return s.client.Eval(ctx, script, keys, args...).Result()
}

// EvalSha is the implementation of redis evalsha command.
func (s *Redis) EvalSha(sha string, keys []string, args ...interface{}) (interface{}, error) {
	return s.EvalShaCtx(context.Background(), sha, keys, args...)
}

// EvalShaCtx is the implementation of redis evalsha command.
func (s *Redis) EvalShaCtx(ctx context.Context, sha string, keys []string,
	args ...interface{}) (val interface{}, err error) {
	return s.client.EvalSha(ctx, sha, keys, args...).Result()
}

// Exists is the implementation of redis exists command.
func (s *Redis) Exists(key string) (bool, error) {
	return s.ExistsCtx(context.Background(), key)
}

// ExistsCtx is the implementation of redis exists command.
func (s *Redis) ExistsCtx(ctx context.Context, key string) (val bool, err error) {
	v, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}

	val = v == 1
	return
}

// Expire is the implementation of redis expire command.
func (s *Redis) Expire(key string, seconds int64) error {
	return s.ExpireCtx(context.Background(), key, seconds)
}

// ExpireCtx is the implementation of redis expire command.
func (s *Redis) ExpireCtx(ctx context.Context, key string, seconds int64) error {
	return s.client.Expire(ctx, key, time.Duration(seconds)*time.Second).Err()
}

// ExpireAt is the implementation of redis expireat command.
func (s *Redis) ExpireAt(key string, expireTime int64) error {
	return s.ExpireAtCtx(context.Background(), key, expireTime)
}

// ExpireAtCtx is the implementation of redis expireat command.
func (s *Redis) ExpireAtCtx(ctx context.Context, key string, expireTime int64) error {
	return s.client.ExpireAt(ctx, key, time.Unix(expireTime, 0)).Err()
}

// ------------------------

// GeoAdd is the implementation of redis geoadd command.
func (s *Redis) GeoAdd(key string, geoLocation ...*GeoLocation) (int64, error) {
	return s.GeoAddCtx(context.Background(), key, geoLocation...)
}

// GeoAddCtx is the implementation of redis geoadd command.
func (s *Redis) GeoAddCtx(ctx context.Context, key string, geoLocation ...*GeoLocation) (
	val int64, err error) {
	return s.client.GeoAdd(ctx, key, geoLocation...).Result()
}

// GeoDist is the implementation of redis geodist command.
func (s *Redis) GeoDist(key, member1, member2, unit string) (float64, error) {
	return s.GeoDistCtx(context.Background(), key, member1, member2, unit)
}

// GeoDistCtx is the implementation of redis geodist command.
func (s *Redis) GeoDistCtx(ctx context.Context, key, member1, member2, unit string) (
	val float64, err error) {
	return s.client.GeoDist(ctx, key, member1, member2, unit).Result()
}

// GeoHash is the implementation of redis geohash command.
func (s *Redis) GeoHash(key string, members ...string) ([]string, error) {
	return s.GeoHashCtx(context.Background(), key, members...)
}

// GeoHashCtx is the implementation of redis geohash command.
func (s *Redis) GeoHashCtx(ctx context.Context, key string, members ...string) (
	val []string, err error) {
	return s.client.GeoHash(ctx, key, members...).Result()
}

// GeoRadius is the implementation of redis georadius command.
func (s *Redis) GeoRadius(key string, longitude, latitude float64, query *GeoRadiusQuery) (
	[]GeoLocation, error) {
	return s.GeoRadiusCtx(context.Background(), key, longitude, latitude, query)
}

// GeoRadiusCtx is the implementation of redis georadius command.
func (s *Redis) GeoRadiusCtx(ctx context.Context, key string, longitude, latitude float64,
	query *GeoRadiusQuery) (val []GeoLocation, err error) {
	return s.client.GeoRadius(ctx, key, longitude, latitude, query).Result()
}

// GeoRadiusByMember is the implementation of redis georadiusbymember command.
func (s *Redis) GeoRadiusByMember(key, member string, query *GeoRadiusQuery) ([]GeoLocation, error) {
	return s.GeoRadiusByMemberCtx(context.Background(), key, member, query)
}

// GeoRadiusByMemberCtx is the implementation of redis georadiusbymember command.
func (s *Redis) GeoRadiusByMemberCtx(ctx context.Context, key, member string,
	query *GeoRadiusQuery) (val []GeoLocation, err error) {
	return s.client.GeoRadiusByMember(ctx, key, member, query).Result()
}

// GeoPos is the implementation of redis geopos command.
func (s *Redis) GeoPos(key string, members ...string) ([]*GeoPos, error) {
	return s.GeoPosCtx(context.Background(), key, members...)
}

// GeoPosCtx is the implementation of redis geopos command.
func (s *Redis) GeoPosCtx(ctx context.Context, key string, members ...string) (
	val []*GeoPos, err error) {
	return s.client.GeoPos(ctx, key, members...).Result()
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

// GetBit is the implementation of redis getbit command.
func (s *Redis) GetBit(key string, offset int64) (int64, error) {
	return s.GetBitCtx(context.Background(), key, offset)
}

// GetBitCtx is the implementation of redis getbit command.
func (s *Redis) GetBitCtx(ctx context.Context, key string, offset int64) (val int64, err error) {
	return s.client.GetBit(ctx, key, offset).Result()
}

// GetSet is the implementation of redis getset command.
func (s *Redis) GetSet(key, value string) (string, error) {
	return s.GetSetCtx(context.Background(), key, value)
}

// GetSetCtx is the implementation of redis getset command.
func (s *Redis) GetSetCtx(ctx context.Context, key, value string) (val string, err error) {
	if val, err = s.client.GetSet(ctx, key, value).Result(); err == red.Nil {
		return val, nil
	}
	return "", err
}

// ------------------------

// HDel is the implementation of redis hdel command.
func (s *Redis) HDel(key string, fields ...string) (bool, error) {
	return s.HDelCtx(context.Background(), key, fields...)
}

// HDelCtx is the implementation of redis hdel command.
func (s *Redis) HDelCtx(ctx context.Context, key string, fields ...string) (val bool, err error) {
	v, err := s.client.HDel(ctx, key, fields...).Result()
	return v >= 1, err
}

// HExists is the implementation of redis hexists command.
func (s *Redis) HExists(key, field string) (bool, error) {
	return s.HExistsCtx(context.Background(), key, field)
}

// HExistsCtx is the implementation of redis hexists command.
func (s *Redis) HExistsCtx(ctx context.Context, key, field string) (val bool, err error) {
	return s.client.HExists(ctx, key, field).Result()
}

// HGet is the implementation of redis hget command.
func (s *Redis) HGet(key, field string) (string, error) {
	return s.HGetCtx(context.Background(), key, field)
}

// HGetCtx is the implementation of redis hget command.
func (s *Redis) HGetCtx(ctx context.Context, key, field string) (val string, err error) {
	return s.client.HGet(ctx, key, field).Result()
}

// HGetAll is the implementation of redis hgetall command.
func (s *Redis) HGetAll(key string) (map[string]string, error) {
	return s.HGetAllCtx(context.Background(), key)
}

// HGetAllCtx is the implementation of redis hgetall command.
func (s *Redis) HGetAllCtx(ctx context.Context, key string) (val map[string]string, err error) {
	return s.client.HGetAll(ctx, key).Result()
}

// HIncrBy is the implementation of redis hincrby command.
func (s *Redis) HIncrBy(key, field string, increment int64) (int64, error) {
	return s.HIncrByCtx(context.Background(), key, field, increment)
}

// HIncrByCtx is the implementation of redis hincrby command.
func (s *Redis) HIncrByCtx(ctx context.Context, key, field string, increment int64) (val int64, err error) {
	return s.client.HIncrBy(ctx, key, field, increment).Result()
}

// HIncrByFloat is the implementation of redis hincrbyfloat command.
func (s *Redis) HIncrByFloat(key, field string, increment float64) (float64, error) {
	return s.HIncrByFloatCtx(context.Background(), key, field, increment)
}

// HIncrByFloatCtx is the implementation of redis hincrbyfloat command.
func (s *Redis) HIncrByFloatCtx(ctx context.Context, key, field string, increment float64) (val float64, err error) {
	return s.client.HIncrByFloat(ctx, key, field, increment).Result()
}

// HKeys is the implementation of redis hkeys command.
func (s *Redis) HKeys(key string) ([]string, error) {
	return s.HKeysCtx(context.Background(), key)
}

// HKeysCtx is the implementation of redis hkeys command.
func (s *Redis) HKeysCtx(ctx context.Context, key string) (val []string, err error) {
	return s.client.HKeys(ctx, key).Result()
}

// HLen is the implementation of redis hlen command.
func (s *Redis) HLen(key string) (int64, error) {
	return s.HLenCtx(context.Background(), key)
}

// HLenCtx is the implementation of redis hlen command.
func (s *Redis) HLenCtx(ctx context.Context, key string) (val int64, err error) {
	return s.client.HLen(ctx, key).Result()
}

// HMGet is the implementation of redis hmget command.
func (s *Redis) HMGet(key string, fields ...string) ([]string, error) {
	return s.HMGetCtx(context.Background(), key, fields...)
}

// HMGetCtx is the implementation of redis hmget command.
func (s *Redis) HMGetCtx(ctx context.Context, key string, fields ...string) (val []string, err error) {
	v, err := s.client.HMGet(ctx, key, fields...).Result()
	val = toStrings(v)
	return
}

// HSet is the implementation of redis hset command.
func (s *Redis) HSet(key, field, value string) error {
	return s.HSetCtx(context.Background(), key, field, value)
}

// HSetCtx is the implementation of redis hset command.
func (s *Redis) HSetCtx(ctx context.Context, key, field, value string) error {
	return s.client.HSet(ctx, key, field, value).Err()
}

// HSetNX is the implementation of redis hsetnx command.
func (s *Redis) HSetNX(key, field, value string) (bool, error) {
	return s.HSetNXCtx(context.Background(), key, field, value)
}

// HSetNXCtx is the implementation of redis hsetnx command.
func (s *Redis) HSetNXCtx(ctx context.Context, key, field, value string) (val bool, err error) {
	return s.client.HSetNX(ctx, key, field, value).Result()
}

// HMSet is the implementation of redis hmset command.
func (s *Redis) HMSet(key string, fieldsAndValues map[string]string) error {
	return s.HMSetCtx(context.Background(), key, fieldsAndValues)
}

// HMSetCtx is the implementation of redis hmset command.
func (s *Redis) HMSetCtx(ctx context.Context, key string, fieldsAndValues map[string]string) error {
	vals := make(map[string]interface{}, len(fieldsAndValues))
	for k, v := range fieldsAndValues {
		vals[k] = v
	}

	return s.client.HMSet(ctx, key, vals).Err()
}

// HScan is the implementation of redis hscan command.
func (s *Redis) HScan(key string, cursor uint64, match string, count int64) (
	keys []string, cur uint64, err error) {
	return s.HScanCtx(context.Background(), key, cursor, match, count)
}

// HScanCtx is the implementation of redis hscan command.
func (s *Redis) HScanCtx(ctx context.Context, key string, cursor uint64, match string, count int64) (
	keys []string, cur uint64, err error) {
	keys, cur, err = s.client.HScan(ctx, key, cursor, match, count).Result()
	return
}

// HVals is the implementation of redis hvals command.
func (s *Redis) HVals(key string) ([]string, error) {
	return s.HValsCtx(context.Background(), key)
}

// HValsCtx is the implementation of redis hvals command.
func (s *Redis) HValsCtx(ctx context.Context, key string) (val []string, err error) {
	val, err = s.client.HVals(ctx, key).Result()
	return
}

// ------------------------

// ------------------------

// ------------------------

// ------------------------
