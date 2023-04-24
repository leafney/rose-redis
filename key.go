package rredis

import (
	"context"
	"time"
)

// Del deletes keys.
func (s *Redis) Del(keys ...string) (int64, error) {
	return s.DelCtx(s.ctx, keys...)
}

// DelCtx deletes keys.
func (s *Redis) DelCtx(ctx context.Context, keys ...string) (val int64, err error) {
	return s.client.Del(ctx, keys...).Result()
}

// Exists is the implementation of redis exists command.
func (s *Redis) Exists(key string) (bool, error) {
	return s.ExistsCtx(s.ctx, key)
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
	return s.ExpireCtx(s.ctx, key, seconds)
}

// ExpireCtx is the implementation of redis expire command.
func (s *Redis) ExpireCtx(ctx context.Context, key string, seconds int64) error {
	return s.client.Expire(ctx, key, time.Duration(seconds)*time.Second).Err()
}

// ExpireAt is the implementation of redis expireat command.
func (s *Redis) ExpireAt(key string, expireTime int64) error {
	return s.ExpireAtCtx(s.ctx, key, expireTime)
}

// ExpireAtCtx is the implementation of redis expireat command.
func (s *Redis) ExpireAtCtx(ctx context.Context, key string, expireTime int64) error {
	return s.client.ExpireAt(ctx, key, time.Unix(expireTime, 0)).Err()
}

// Keys is the implementation of redis keys command.
func (s *Redis) Keys(pattern string) ([]string, error) {
	return s.KeysCtx(s.ctx, pattern)
}

// KeysCtx is the implementation of redis keys command.
func (s *Redis) KeysCtx(ctx context.Context, pattern string) (val []string, err error) {
	return s.client.Keys(ctx, pattern).Result()
}

// Persist is the implementation of redis persist command.
func (s *Redis) Persist(key string) (bool, error) {
	return s.PersistCtx(s.ctx, key)
}

// PersistCtx is the implementation of redis persist command.
func (s *Redis) PersistCtx(ctx context.Context, key string) (val bool, err error) {
	return s.client.Persist(ctx, key).Result()
}

// TTL is the implementation of redis ttl command.
func (s *Redis) TTL(key string) (int64, error) {
	return s.TTLCtx(context.Background(), key)
}

// TTLCtx is the implementation of redis ttl command.
func (s *Redis) TTLCtx(ctx context.Context, key string) (val int64, err error) {
	duration, err := s.client.TTL(ctx, key).Result()
	if err != nil {
		return -1, err
	}
	val = int64(duration.Seconds())
	return
}

// Scan is the implementation of redis scan command.
func (s *Redis) Scan(cursor uint64, match string, count int64) (keys []string, cur uint64, err error) {
	return s.ScanCtx(context.Background(), cursor, match, count)
}

// ScanCtx is the implementation of redis scan command.
func (s *Redis) ScanCtx(ctx context.Context, cursor uint64, match string, count int64) (
	keys []string, cur uint64, err error) {
	keys, cur, err = s.client.Scan(ctx, cursor, match, count).Result()
	return
}
