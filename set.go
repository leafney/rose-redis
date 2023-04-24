package rredis

import "context"

// SAdd is the implementation of redis sadd command.
func (s *Redis) SAdd(key string, values ...interface{}) (int64, error) {
	return s.SAddCtx(context.Background(), key, values...)
}

// SAddCtx is the implementation of redis sadd command.
func (s *Redis) SAddCtx(ctx context.Context, key string, values ...interface{}) (val int64, err error) {
	return s.client.SAdd(ctx, key, values...).Result()
}

// SCard is the implementation of redis scard command.
func (s *Redis) SCard(key string) (int64, error) {
	return s.SCardCtx(context.Background(), key)
}

// SCardCtx is the implementation of redis scard command.
func (s *Redis) SCardCtx(ctx context.Context, key string) (val int64, err error) {
	return s.client.SCard(ctx, key).Result()
}

// SScan is the implementation of redis sscan command.
func (s *Redis) SScan(key string, cursor uint64, match string, count int64) (
	keys []string, cur uint64, err error) {
	return s.SScanCtx(context.Background(), key, cursor, match, count)
}

// SScanCtx is the implementation of redis sscan command.
func (s *Redis) SScanCtx(ctx context.Context, key string, cursor uint64, match string, count int64) (
	keys []string, cur uint64, err error) {
	return s.client.SScan(ctx, key, cursor, match, count).Result()
}

// SDiff is the implementation of redis sdiff command.
func (s *Redis) SDiff(keys ...string) ([]string, error) {
	return s.SDiffCtx(context.Background(), keys...)
}

// SDiffCtx is the implementation of redis sdiff command.
func (s *Redis) SDiffCtx(ctx context.Context, keys ...string) (val []string, err error) {
	return s.client.SDiff(ctx, keys...).Result()
}

// SDiffStore is the implementation of redis sdiffstore command.
func (s *Redis) SDiffStore(destination string, keys ...string) (int64, error) {
	return s.SDiffStoreCtx(context.Background(), destination, keys...)
}

// SDiffStoreCtx is the implementation of redis sdiffstore command.
func (s *Redis) SDiffStoreCtx(ctx context.Context, destination string, keys ...string) (
	val int64, err error) {
	return s.client.SDiffStore(ctx, destination, keys...).Result()
}

// SInter is the implementation of redis sinter command.
func (s *Redis) SInter(keys ...string) ([]string, error) {
	return s.SInterCtx(context.Background(), keys...)
}

// SInterCtx is the implementation of redis sinter command.
func (s *Redis) SInterCtx(ctx context.Context, keys ...string) (val []string, err error) {
	return s.client.SInter(ctx, keys...).Result()
}

// SInterStore is the implementation of redis sinterstore command.
func (s *Redis) SInterStore(destination string, keys ...string) (int64, error) {
	return s.SInterStoreCtx(context.Background(), destination, keys...)
}

// SInterStoreCtx is the implementation of redis sinterstore command.
func (s *Redis) SInterStoreCtx(ctx context.Context, destination string, keys ...string) (
	val int64, err error) {
	return s.client.SInterStore(ctx, destination, keys...).Result()
}

// SIsMember is the implementation of redis sismember command.
func (s *Redis) SIsMember(key string, value interface{}) (bool, error) {
	return s.SIsMemberCtx(context.Background(), key, value)
}

// SIsMemberCtx is the implementation of redis sismember command.
func (s *Redis) SIsMemberCtx(ctx context.Context, key string, value interface{}) (val bool, err error) {
	return s.client.SIsMember(ctx, key, value).Result()
}

// SMembers is the implementation of redis smembers command.
func (s *Redis) SMembers(key string) ([]string, error) {
	return s.SMembersCtx(context.Background(), key)
}

// SMembersCtx is the implementation of redis smembers command.
func (s *Redis) SMembersCtx(ctx context.Context, key string) (val []string, err error) {
	return s.client.SMembers(ctx, key).Result()
}

// SPop is the implementation of redis spop command.
func (s *Redis) SPop(key string) (string, error) {
	return s.SPopCtx(context.Background(), key)
}

// SPopCtx is the implementation of redis spop command.
func (s *Redis) SPopCtx(ctx context.Context, key string) (val string, err error) {
	return s.client.SPop(ctx, key).Result()
}

// SRandMember is the implementation of redis srandmember command.
func (s *Redis) SRandMember(key string) (string, error) {
	return s.SRandMemberCtx(context.Background(), key)
}

// SRandMemberCtx is the implementation of redis srandmember command.
func (s *Redis) SRandMemberCtx(ctx context.Context, key string) (val string, err error) {
	return s.client.SRandMember(ctx, key).Result()
}

// SRandMemberN is the implementation of redis SRandMemberN command.
func (s *Redis) SRandMemberN(key string, count int64) ([]string, error) {
	return s.SRandMemberNCtx(context.Background(), key, count)
}

// SRandMemberNCtx is the implementation of redis SRandMemberN command.
func (s *Redis) SRandMemberNCtx(ctx context.Context, key string, count int64) (val []string, err error) {
	return s.client.SRandMemberN(ctx, key, count).Result()
}

// SRem is the implementation of redis srem command.
func (s *Redis) SRem(key string, values ...interface{}) (int64, error) {
	return s.SRemCtx(context.Background(), key, values...)
}

// SRemCtx is the implementation of redis srem command.
func (s *Redis) SRemCtx(ctx context.Context, key string, values ...interface{}) (val int64, err error) {
	return s.client.SRem(ctx, key, values...).Result()
}

// SUnion is the implementation of redis sunion command.
func (s *Redis) SUnion(keys ...string) ([]string, error) {
	return s.SUnionCtx(context.Background(), keys...)
}

// SUnionCtx is the implementation of redis sunion command.
func (s *Redis) SUnionCtx(ctx context.Context, keys ...string) (val []string, err error) {
	return s.client.SUnion(ctx, keys...).Result()
}

// SUnionStore is the implementation of redis sunionstore command.
func (s *Redis) SUnionStore(destination string, keys ...string) (int64, error) {
	return s.SUnionStoreCtx(context.Background(), destination, keys...)
}

// SUnionStoreCtx is the implementation of redis sunionstore command.
func (s *Redis) SUnionStoreCtx(ctx context.Context, destination string, keys ...string) (
	val int64, err error) {
	return s.client.SUnionStore(ctx, destination, keys...).Result()
}
