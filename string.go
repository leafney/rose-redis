package rredis

import (
	"context"
	red "github.com/go-redis/redis/v8"
	"time"
)

// Set is the implementation of redis set command.
func (s *Redis) Set(key, value string) error {
	return s.SetCtx(s.ctx, key, value)
}

// SetCtx is the implementation of redis set command.
func (s *Redis) SetCtx(ctx context.Context, key, value string) error {
	return s.client.Set(ctx, key, value, 0).Err()
}

// Get is the implementation of redis get command.
func (s *Redis) Get(key string) (string, error) {
	return s.GetCtx(s.ctx, key)
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

// TODO Getrange
// TODO Setrange

// GetSet is the implementation of redis getset command.
func (s *Redis) GetSet(key, value string) (string, error) {
	return s.GetSetCtx(s.ctx, key, value)
}

// GetSetCtx is the implementation of redis getset command.
func (s *Redis) GetSetCtx(ctx context.Context, key, value string) (val string, err error) {
	if val, err = s.client.GetSet(ctx, key, value).Result(); err == red.Nil {
		return val, nil
	}
	return "", err
}

// GetBit is the implementation of redis getbit command.
func (s *Redis) GetBit(key string, offset int64) (int64, error) {
	return s.GetBitCtx(s.ctx, key, offset)
}

// GetBitCtx is the implementation of redis getbit command.
func (s *Redis) GetBitCtx(ctx context.Context, key string, offset int64) (val int64, err error) {
	return s.client.GetBit(ctx, key, offset).Result()
}

// MGet is the implementation of redis mget command.
func (s *Redis) MGet(keys ...string) ([]string, error) {
	return s.MGetCtx(s.ctx, keys...)
}

// MGetCtx is the implementation of redis mget command.
func (s *Redis) MGetCtx(ctx context.Context, keys ...string) (val []string, err error) {
	v, err := s.client.MGet(ctx, keys...).Result()
	val = toStrings(v)
	return
}

// MGet is the implementation of redis mset command.
func (s *Redis) MSet(values ...interface{}) (string, error) {
	return s.MSetCtx(s.ctx, values)
}

// MGetCtx is the implementation of redis mset command.
func (s *Redis) MSetCtx(ctx context.Context, values ...interface{}) (val string, err error) {
	return s.client.MSet(ctx, values).Result()
}

// MSetNx is the implementation of redis msetnx command.
func (s *Redis) MSetNx(values ...interface{}) (bool, error) {
	return s.MSetNxCtx(s.ctx, values)
}

// MSetNxCtx is the implementation of redis msetnx command.
func (s *Redis) MSetNxCtx(ctx context.Context, values ...interface{}) (val bool, err error) {
	return s.client.MSetNX(ctx, values).Result()
}

// SetBit is the implementation of redis setbit command.
func (s *Redis) SetBit(key string, offset int64, value int) (int64, error) {
	return s.SetBitCtx(s.ctx, key, offset, value)
}

// SetBitCtx is the implementation of redis setbit command.
func (s *Redis) SetBitCtx(ctx context.Context, key string, offset int64, value int) (val int64, err error) {
	return s.client.SetBit(ctx, key, offset, value).Result()
}

// SetEx is the implementation of redis setex command.
func (s *Redis) SetEx(key, value string, seconds int64) error {
	return s.SetExCtx(s.ctx, key, value, seconds)
}

// SetExCtx is the implementation of redis setex command.
func (s *Redis) SetExCtx(ctx context.Context, key, value string, seconds int64) error {
	return s.client.Set(ctx, key, value, time.Duration(seconds)*time.Second).Err()
}

// SetNx is the implementation of redis setnx command.
func (s *Redis) SetNx(key, value string) (bool, error) {
	return s.SetNxCtx(s.ctx, key, value)
}

// SetNxCtx is the implementation of redis setnx command.
func (s *Redis) SetNxCtx(ctx context.Context, key, value string) (val bool, err error) {
	return s.client.SetNX(ctx, key, value, 0).Result()
}

// SetNxEx is the implementation of redis setnx command with expire.
func (s *Redis) SetNxEx(key, value string, seconds int64) (bool, error) {
	return s.SetNxExCtx(s.ctx, key, value, seconds)
}

// SetNxExCtx is the implementation of redis setnx command with expire.
func (s *Redis) SetNxExCtx(ctx context.Context, key, value string, seconds int64) (val bool, err error) {
	return s.client.SetNX(ctx, key, value, time.Duration(seconds)*time.Second).Result()
}

// TODO strlen

// Incr is the implementation of redis incr command.
func (s *Redis) Incr(key string) (int64, error) {
	return s.IncrCtx(s.ctx, key)
}

// IncrCtx is the implementation of redis incr command.
func (s *Redis) IncrCtx(ctx context.Context, key string) (val int64, err error) {
	return s.client.Incr(ctx, key).Result()
}

// IncrBy is the implementation of redis incrby command.
func (s *Redis) IncrBy(key string, increment int64) (int64, error) {
	return s.IncrByCtx(s.ctx, key, increment)
}

// IncrByCtx is the implementation of redis incrby command.
func (s *Redis) IncrByCtx(ctx context.Context, key string, increment int64) (val int64, err error) {
	return s.client.IncrBy(ctx, key, increment).Result()
}

// IncrByFloat is the implementation of redis incrbyfloat command.
func (s *Redis) IncrByFloat(key string, increment float64) (float64, error) {
	return s.IncrByFloatCtx(s.ctx, key, increment)
}

// IncrByFloatCtx is the implementation of redis incrbyfloat command.
func (s *Redis) IncrByFloatCtx(ctx context.Context, key string, increment float64) (val float64, err error) {
	return s.client.IncrByFloat(ctx, key, increment).Result()
}

// Decr is the implementation of redis decr command.
func (s *Redis) Decr(key string) (int64, error) {
	return s.DecrCtx(s.ctx, key)
}

// DecrCtx is the implementation of redis decr command.
func (s *Redis) DecrCtx(ctx context.Context, key string) (val int64, err error) {
	return s.client.Decr(ctx, key).Result()
}

// DecrBy is the implementation of redis decrby command.
func (s *Redis) DecrBy(key string, decrement int64) (int64, error) {
	return s.DecrByCtx(s.ctx, key, decrement)
}

// DecrByCtx is the implementation of redis decrby command.
func (s *Redis) DecrByCtx(ctx context.Context, key string, decrement int64) (val int64, err error) {
	return s.client.DecrBy(ctx, key, decrement).Result()
}

// Append is the implementation of redis append command.
func (s *Redis) Append(key string, value string) (val int64, err error) {
	return s.AppendCtx(s.ctx, key, value)
}

// AppendCtx is the implementation of redis append command.
func (s *Redis) AppendCtx(ctx context.Context, key string, value string) (val int64, err error) {
	return s.client.Append(ctx, key, value).Result()
}
