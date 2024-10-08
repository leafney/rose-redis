package rredis

import (
	"context"
	red "github.com/redis/go-redis/v9"
)

// BitCount is redis bitcount command implementation.
func (s *Redis) BitCount(key string, start, end int64) (int64, error) {
	return s.BitCountCtx(s.ctx, key, start, end)
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
	return s.BitOpAndCtx(s.ctx, destKey, keys...)
}

// BitOpAndCtx is redis bit operation (and) command implementation.
func (s *Redis) BitOpAndCtx(ctx context.Context, destKey string, keys ...string) (val int64, err error) {
	return s.client.BitOpAnd(ctx, destKey, keys...).Result()
}

// BitOpNot is redis bit operation (not) command implementation.
func (s *Redis) BitOpNot(destKey, key string) (int64, error) {
	return s.BitOpNotCtx(s.ctx, destKey, key)
}

// BitOpNotCtx is redis bit operation (not) command implementation.
func (s *Redis) BitOpNotCtx(ctx context.Context, destKey, key string) (val int64, err error) {
	return s.client.BitOpNot(ctx, destKey, key).Result()
}

// BitOpOr is redis bit operation (or) command implementation.
func (s *Redis) BitOpOr(destKey string, keys ...string) (int64, error) {
	return s.BitOpOrCtx(s.ctx, destKey, keys...)
}

// BitOpOrCtx is redis bit operation (or) command implementation.
func (s *Redis) BitOpOrCtx(ctx context.Context, destKey string, keys ...string) (val int64, err error) {
	return s.client.BitOpOr(ctx, destKey, keys...).Result()
}

// BitOpXor is redis bit operation (xor) command implementation.
func (s *Redis) BitOpXor(destKey string, keys ...string) (int64, error) {
	return s.BitOpXorCtx(s.ctx, destKey, keys...)
}

// BitOpXorCtx is redis bit operation (xor) command implementation.
func (s *Redis) BitOpXorCtx(ctx context.Context, destKey string, keys ...string) (val int64, err error) {
	return s.client.BitOpXor(ctx, destKey, keys...).Result()
}

// BitPos is redis bitpos command implementation.
func (s *Redis) BitPos(key string, bit, start, end int64) (int64, error) {
	return s.BitPosCtx(s.ctx, key, bit, start, end)
}

// BitPosCtx is redis bitpos command implementation.
func (s *Redis) BitPosCtx(ctx context.Context, key string, bit, start, end int64) (val int64, err error) {
	return s.client.BitPos(ctx, key, bit, start, end).Result()
}
