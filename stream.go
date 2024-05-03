/**
 * @Author:      leafney
 * @GitHub:      https://github.com/leafney
 * @Project:     rose-redis
 * @Date:        2024-05-03 11:18
 * @Description:
 */

package rredis

import (
	"context"
	"github.com/go-redis/redis/v8"
)

func (s *Redis) XAdd(ctx context.Context, a *redis.XAddArgs) (string, error) {
	return s.client.XAdd(ctx, a).Result()
}

func (s *Redis) XDel(ctx context.Context, stream string, ids ...string) error {
	return s.client.XDel(ctx, stream, ids...).Err()
}

func (s *Redis) XLen(ctx context.Context, stream string) (int64, error) {
	return s.client.XLen(ctx, stream).Result()
}

func (s *Redis) XGroupCreateMkStream(ctx context.Context, stream, group, start string) error {
	return s.client.XGroupCreateMkStream(ctx, stream, group, start).Err()
}

func (s *Redis) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) ([]redis.XStream, error) {
	return s.client.XReadGroup(ctx, a).Result()
}

func (s *Redis) XAck(ctx context.Context, stream, group string, ids ...string) error {
	return s.client.XAck(ctx, stream, group, ids...).Err()
}

func (s *Redis) XPending(ctx context.Context, stream, group string) (*redis.XPending, error) {
	return s.client.XPending(ctx, stream, group).Result()
}

func (s *Redis) XTrimMaxLenApprox(ctx context.Context, key string, maxLen, limit int64) error {
	return s.client.XTrimMaxLenApprox(ctx, key, maxLen, limit).Err()
}

func (s *Redis) XInfoStream(ctx context.Context, key string) (*redis.XInfoStream, error) {
	return s.client.XInfoStream(ctx, key).Result()
}
