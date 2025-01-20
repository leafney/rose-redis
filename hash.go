package rredis

import "context"

// HDel is the implementation of redis hdel command.
func (s *Redis) HDel(key string, fields ...string) (bool, error) {
	return s.HDelCtx(s.ctx, key, fields...)
}

// HDelCtx is the implementation of redis hdel command.
func (s *Redis) HDelCtx(ctx context.Context, key string, fields ...string) (val bool, err error) {
	v, err := s.client.HDel(ctx, key, fields...).Result()
	return v >= 1, err
}

// HExists is the implementation of redis hexists command.
func (s *Redis) HExists(key, field string) (bool, error) {
	return s.HExistsCtx(s.ctx, key, field)
}

// HExistsCtx is the implementation of redis hexists command.
func (s *Redis) HExistsCtx(ctx context.Context, key, field string) (val bool, err error) {
	return s.client.HExists(ctx, key, field).Result()
}

// HGet is the implementation of redis hget command.
func (s *Redis) HGet(key, field string) (string, error) {
	return s.HGetCtx(s.ctx, key, field)
}

// HGetCtx is the implementation of redis hget command.
func (s *Redis) HGetCtx(ctx context.Context, key, field string) (val string, err error) {
	return s.client.HGet(ctx, key, field).Result()
}

// HGetAll is the implementation of redis hgetall command.
func (s *Redis) HGetAll(key string) (map[string]string, error) {
	return s.HGetAllCtx(s.ctx, key)
}

// HGetAllCtx is the implementation of redis hgetall command.
func (s *Redis) HGetAllCtx(ctx context.Context, key string) (val map[string]string, err error) {
	return s.client.HGetAll(ctx, key).Result()
}

// HIncrBy is the implementation of redis hincrby command.
func (s *Redis) HIncrBy(key, field string, increment int64) (int64, error) {
	return s.HIncrByCtx(s.ctx, key, field, increment)
}

// HIncrByCtx is the implementation of redis hincrby command.
func (s *Redis) HIncrByCtx(ctx context.Context, key, field string, increment int64) (val int64, err error) {
	return s.client.HIncrBy(ctx, key, field, increment).Result()
}

// HIncrByFloat is the implementation of redis hincrbyfloat command.
func (s *Redis) HIncrByFloat(key, field string, increment float64) (float64, error) {
	return s.HIncrByFloatCtx(s.ctx, key, field, increment)
}

// HIncrByFloatCtx is the implementation of redis hincrbyfloat command.
func (s *Redis) HIncrByFloatCtx(ctx context.Context, key, field string, increment float64) (val float64, err error) {
	return s.client.HIncrByFloat(ctx, key, field, increment).Result()
}

// HKeys is the implementation of redis hkeys command.
func (s *Redis) HKeys(key string) ([]string, error) {
	return s.HKeysCtx(s.ctx, key)
}

// HKeysCtx is the implementation of redis hkeys command.
func (s *Redis) HKeysCtx(ctx context.Context, key string) (val []string, err error) {
	return s.client.HKeys(ctx, key).Result()
}

// HLen is the implementation of redis hlen command.
func (s *Redis) HLen(key string) (int64, error) {
	return s.HLenCtx(s.ctx, key)
}

// HLenCtx is the implementation of redis hlen command.
func (s *Redis) HLenCtx(ctx context.Context, key string) (val int64, err error) {
	return s.client.HLen(ctx, key).Result()
}

// HMGet is the implementation of redis hmget command.
func (s *Redis) HMGet(key string, fields ...string) ([]string, error) {
	return s.HMGetCtx(s.ctx, key, fields...)
}

// HMGetCtx is the implementation of redis hmget command.
func (s *Redis) HMGetCtx(ctx context.Context, key string, fields ...string) (val []string, err error) {
	v, err := s.client.HMGet(ctx, key, fields...).Result()
	val = toStrings(v)
	return
}

// HSet is the implementation of redis hset command.
func (s *Redis) HSet(key, field string, value interface{}) error {
	return s.HSetCtx(s.ctx, key, field, value)
}

// HSetCtx is the implementation of redis hset command.
func (s *Redis) HSetCtx(ctx context.Context, key, field string, value interface{}) error {
	return s.client.HSet(ctx, key, field, value).Err()
}

// HSetNX is the implementation of redis hsetnx command.
func (s *Redis) HSetNX(key, field string, value interface{}) (bool, error) {
	return s.HSetNXCtx(s.ctx, key, field, value)
}

// HSetNXCtx is the implementation of redis hsetnx command.
func (s *Redis) HSetNXCtx(ctx context.Context, key, field string, value interface{}) (val bool, err error) {
	return s.client.HSetNX(ctx, key, field, value).Result()
}

// HMSet is the implementation of redis hmset command.
func (s *Redis) HMSet(key string, fieldsAndValues map[string]interface{}) error {
	return s.HMSetCtx(s.ctx, key, fieldsAndValues)
}

// HMSetCtx is the implementation of redis hmset command.
func (s *Redis) HMSetCtx(ctx context.Context, key string, fieldsAndValues map[string]interface{}) error {
	vals := make(map[string]interface{}, len(fieldsAndValues))
	for k, v := range fieldsAndValues {
		vals[k] = v
	}

	return s.client.HMSet(ctx, key, vals).Err()
}

// HScan is the implementation of redis hscan command.
func (s *Redis) HScan(key string, cursor uint64, match string, count int64) (
	keys []string, cur uint64, err error) {
	return s.HScanCtx(s.ctx, key, cursor, match, count)
}

// HScanCtx is the implementation of redis hscan command.
func (s *Redis) HScanCtx(ctx context.Context, key string, cursor uint64, match string, count int64) (
	keys []string, cur uint64, err error) {
	keys, cur, err = s.client.HScan(ctx, key, cursor, match, count).Result()
	return
}

// HVals is the implementation of redis hvals command.
func (s *Redis) HVals(key string) ([]string, error) {
	return s.HValsCtx(s.ctx, key)
}

// HValsCtx is the implementation of redis hvals command.
func (s *Redis) HValsCtx(ctx context.Context, key string) (val []string, err error) {
	val, err = s.client.HVals(ctx, key).Result()
	return
}
