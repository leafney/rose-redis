package rredis

import (
	"context"
)

/*
// Blpop uses passed in redis connection to execute blocking queries.
// Doesn't benefit from pooling redis connections of blocking queries
func (s *Redis) Blpop(node RedisNode, key string) (string, error) {
	return s.BlpopCtx(s.ctx, node, key)
}

// BlpopCtx uses passed in redis connection to execute blocking queries.
// Doesn't benefit from pooling redis connections of blocking queries
func (s *Redis) BlpopCtx(ctx context.Context, node RedisNode, key string) (string, error) {
	return s.BlpopWithTimeoutCtx(ctx, node, blockingQueryTimeout, key)
}

// BlpopEx uses passed in redis connection to execute blpop command.
// The difference against Blpop is that this method returns a bool to indicate success.
func (s *Redis) BlpopEx(node RedisNode, key string) (string, bool, error) {
	return s.BlpopExCtx(s.ctx, node, key)
}

// BlpopExCtx uses passed in redis connection to execute blpop command.
// The difference against Blpop is that this method returns a bool to indicate success.
func (s *Redis) BlpopExCtx(ctx context.Context, node RedisNode, key string) (string, bool, error) {
	if node == nil {
		return "", false, ErrNilNode
	}

	vals, err := node.BLPop(ctx, blockingQueryTimeout, key).Result()
	if err != nil {
		return "", false, err
	}

	if len(vals) < 2 {
		return "", false, fmt.Errorf("no value on key: %s", key)
	}

	return vals[1], true, nil
}

// BlpopWithTimeout uses passed in redis connection to execute blpop command.
// Control blocking query timeout
func (s *Redis) BlpopWithTimeout(node RedisNode, timeout time.Duration, key string) (string, error) {
	return s.BlpopWithTimeoutCtx(s.ctx, node, timeout, key)
}

// BlpopWithTimeoutCtx uses passed in redis connection to execute blpop command.
// Control blocking query timeout
func (s *Redis) BlpopWithTimeoutCtx(ctx context.Context, node RedisNode, timeout time.Duration,
	key string) (string, error) {
	if node == nil {
		return "", ErrNilNode
	}

	vals, err := node.BLPop(ctx, timeout, key).Result()
	if err != nil {
		return "", err
	}

	if len(vals) < 2 {
		return "", fmt.Errorf("no value on key: %s", key)
	}

	return vals[1], nil
}

*/

// LIndex is the implementation of redis lindex command.
func (s *Redis) LIndex(key string, index int64) (string, error) {
	return s.LIndexCtx(s.ctx, key, index)
}

// LIndexCtx is the implementation of redis lindex command.
func (s *Redis) LIndexCtx(ctx context.Context, key string, index int64) (val string, err error) {
	return s.client.LIndex(ctx, key, index).Result()
}

// TODO Linsert

// LLen is the implementation of redis llen command.
func (s *Redis) LLen(key string) (int64, error) {
	return s.LLenCtx(s.ctx, key)
}

// LLenCtx is the implementation of redis llen command.
func (s *Redis) LLenCtx(ctx context.Context, key string) (val int64, err error) {
	return s.client.LLen(ctx, key).Result()
}

// LPop is the implementation of redis lpop command.
func (s *Redis) LPop(key string) (string, error) {
	return s.LPopCtx(s.ctx, key)
}

// LPopCtx is the implementation of redis lpop command.
func (s *Redis) LPopCtx(ctx context.Context, key string) (val string, err error) {
	return s.client.LPop(ctx, key).Result()
}

// LPopCount is the implementation of redis lpopCount command.
func (s *Redis) LPopCount(key string, count int) ([]string, error) {
	return s.LPopCountCtx(s.ctx, key, count)
}

// LPopCountCtx is the implementation of redis lpopCount command.
func (s *Redis) LPopCountCtx(ctx context.Context, key string, count int) (val []string, err error) {
	return s.client.LPopCount(ctx, key, count).Result()
}

// LPush is the implementation of redis lpush command.
func (s *Redis) LPush(key string, values ...interface{}) (int64, error) {
	return s.LPushCtx(s.ctx, key, values...)
}

// LPushCtx is the implementation of redis lpush command.
func (s *Redis) LPushCtx(ctx context.Context, key string, values ...interface{}) (val int64, err error) {
	return s.client.LPush(ctx, key, values...).Result()
}

// LRange is the implementation of redis lrange command.
func (s *Redis) LRange(key string, start, stop int64) ([]string, error) {
	return s.LRangeCtx(s.ctx, key, start, stop)
}

// LRangeCtx is the implementation of redis lrange command.
func (s *Redis) LRangeCtx(ctx context.Context, key string, start, stop int64) (val []string, err error) {
	return s.client.LRange(ctx, key, start, stop).Result()
}

// LRem is the implementation of redis lrem command.
func (s *Redis) LRem(key string, count int64, value string) (int64, error) {
	return s.LRemCtx(s.ctx, key, count, value)
}

// LRemCtx is the implementation of redis lrem command.
func (s *Redis) LRemCtx(ctx context.Context, key string, count int64, value string) (val int64, err error) {
	return s.client.LRem(ctx, key, count, value).Result()
}

// TODO Lset

// LTrim is the implementation of redis ltrim command.
func (s *Redis) LTrim(key string, start, stop int64) error {
	return s.LTrimCtx(s.ctx, key, start, stop)
}

// LTrimCtx is the implementation of redis ltrim command.
func (s *Redis) LTrimCtx(ctx context.Context, key string, start, stop int64) error {
	return s.client.LTrim(ctx, key, start, stop).Err()
}

// RPop is the implementation of redis rpop command.
func (s *Redis) RPop(key string) (string, error) {
	return s.RPopCtx(s.ctx, key)
}

// RPopCtx is the implementation of redis rpop command.
func (s *Redis) RPopCtx(ctx context.Context, key string) (val string, err error) {
	return s.client.RPop(ctx, key).Result()
}

// RPopCount is the implementation of redis rpopCount command.
func (s *Redis) RPopCount(key string, count int) ([]string, error) {
	return s.RPopCountCtx(s.ctx, key, count)
}

// RPopCountCtx is the implementation of redis rpopCount command.
func (s *Redis) RPopCountCtx(ctx context.Context, key string, count int) (val []string, err error) {
	return s.client.RPopCount(ctx, key, count).Result()
}

// RPush is the implementation of redis rpush command.
func (s *Redis) RPush(key string, values ...interface{}) (int64, error) {
	return s.RPushCtx(s.ctx, key, values...)
}

// RPushCtx is the implementation of redis rpush command.
func (s *Redis) RPushCtx(ctx context.Context, key string, values ...interface{}) (val int64, err error) {
	return s.client.RPush(ctx, key, values...).Result()
}
