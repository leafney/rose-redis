package rredis

import "context"

// LLen is the implementation of redis llen command.
func (s *Redis) LLen(key string) (int64, error) {
	return s.LLenCtx(s.ctx, key)
}

// LLenCtx is the implementation of redis llen command.
func (s *Redis) LLenCtx(ctx context.Context, key string) (val int64, err error) {
	return s.client.LLen(ctx, key).Result()
}

// LIndex is the implementation of redis lindex command.
func (s *Redis) LIndex(key string, index int64) (string, error) {
	return s.LIndexCtx(s.ctx, key, index)
}

// LindexCtx is the implementation of redis lindex command.
func (s *Redis) LIndexCtx(ctx context.Context, key string, index int64) (val string, err error) {
	return s.client.LIndex(ctx, key, index).Result()
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

// LpopCountCtx is the implementation of redis lpopCount command.
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

// LTrim is the implementation of redis ltrim command.
func (s *Redis) LTrim(key string, start, stop int64) error {
	return s.LTrimCtx(s.ctx, key, start, stop)
}

// LTrimCtx is the implementation of redis ltrim command.
func (s *Redis) LTrimCtx(ctx context.Context, key string, start, stop int64) error {
	return s.client.LTrim(ctx, key, start, stop).Err()
}
