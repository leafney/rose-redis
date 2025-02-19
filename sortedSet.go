package rredis

import (
	"context"
	red "github.com/redis/go-redis/v9"
	"strconv"
)

// ZAdd is the implementation of redis zadd command.
func (s *Redis) ZAdd(key string, score int64, value string) (bool, error) {
	return s.ZAddCtx(s.ctx, key, score, value)
}

// ZAddCtx is the implementation of redis zadd command.
func (s *Redis) ZAddCtx(ctx context.Context, key string, score int64, value string) (
	val bool, err error) {
	return s.ZAddFloatCtx(ctx, key, float64(score), value)
}

// ZAddFloat is the implementation of redis zadd command.
func (s *Redis) ZAddFloat(key string, score float64, value string) (bool, error) {
	return s.ZAddFloatCtx(s.ctx, key, score, value)
}

// ZAddFloatCtx is the implementation of redis zadd command.
func (s *Redis) ZAddFloatCtx(ctx context.Context, key string, score float64, value string) (
	val bool, err error) {
	v, err := s.client.ZAdd(ctx, key, red.Z{
		Score:  score,
		Member: value,
	}).Result()

	val = v == 1
	return
}

// ZAdds is the implementation of redis zadds command.
func (s *Redis) ZAdds(key string, ps ...Pair) (int64, error) {
	return s.ZAddsCtx(s.ctx, key, ps...)
}

// ZAddsCtx is the implementation of redis zadds command.
func (s *Redis) ZAddsCtx(ctx context.Context, key string, ps ...Pair) (val int64, err error) {
	zs := make([]red.Z, 0)
	for _, p := range ps {
		z := red.Z{Score: float64(p.Score), Member: p.Member}
		zs = append(zs, z)
	}

	return s.client.ZAdd(ctx, key, zs...).Result()
}

// ZCard is the implementation of redis zcard command.
func (s *Redis) ZCard(key string) (int64, error) {
	return s.ZCardCtx(s.ctx, key)
}

// ZCardCtx is the implementation of redis zcard command.
func (s *Redis) ZCardCtx(ctx context.Context, key string) (val int64, err error) {
	return s.client.ZCard(ctx, key).Result()
}

// ZCount is the implementation of redis zcount command.
func (s *Redis) ZCount(key string, min, max string) (int64, error) {
	return s.ZCountCtx(s.ctx, key, min, max)
}

// ZCountCtx is the implementation of redis zcount command.
func (s *Redis) ZCountCtx(ctx context.Context, key string, min, max string) (val int64, err error) {
	return s.client.ZCount(ctx, key, min, max).Result()
}

// ZIncrBy is the implementation of redis zincrby command.
func (s *Redis) ZIncrBy(key string, increment int64, field string) (int64, error) {
	return s.ZIncrByCtx(s.ctx, key, increment, field)
}

// ZIncrByCtx is the implementation of redis zincrby command.
func (s *Redis) ZIncrByCtx(ctx context.Context, key string, increment int64, field string) (
	val int64, err error) {
	v, err := s.ZIncrByFloatCtx(ctx, key, float64(increment), field)
	val = int64(v)
	return
}

// ZIncrByFloat is the implementation of redis zincrby command.
func (s *Redis) ZIncrByFloat(key string, increment float64, field string) (float64, error) {
	return s.ZIncrByFloatCtx(s.ctx, key, increment, field)
}

// ZIncrByFloatCtx is the implementation of redis zincrby command.
func (s *Redis) ZIncrByFloatCtx(ctx context.Context, key string, increment float64, field string) (
	val float64, err error) {
	return s.client.ZIncrBy(ctx, key, increment, field).Result()
}

// TODO Zinterstore

// TODO ZLEXCOUNT

// ZScore is the implementation of redis zscore command.
func (s *Redis) ZScore(key, value string) (int64, error) {
	return s.ZScoreCtx(s.ctx, key, value)
}

// ZScoreCtx is the implementation of redis zscore command.
func (s *Redis) ZScoreCtx(ctx context.Context, key, value string) (val int64, err error) {
	v, err := s.ZScoreFloatCtx(ctx, key, value)
	val = int64(v)
	return
}

// ZScoreFloat is the implementation of redis zscore command score by float.
func (s *Redis) ZScoreFloat(key, value string) (float64, error) {
	return s.ZScoreFloatCtx(s.ctx, key, value)
}

// ZScoreFloatCtx is the implementation of redis zscore command score by float.
func (s *Redis) ZScoreFloatCtx(ctx context.Context, key, value string) (val float64, err error) {
	s.client.ZScore(ctx, key, value).Result()
	return
}

// ZScan is the implementation of redis zscan command.
func (s *Redis) ZScan(key string, cursor uint64, match string, count int64) (
	keys []string, cur uint64, err error) {
	return s.ZScanCtx(s.ctx, key, cursor, match, count)
}

// ZScanCtx is the implementation of redis zscan command.
func (s *Redis) ZScanCtx(ctx context.Context, key string, cursor uint64, match string, count int64) (
	keys []string, cur uint64, err error) {
	return s.client.ZScan(ctx, key, cursor, match, count).Result()
}

// ZRank is the implementation of redis zrank command.
func (s *Redis) ZRank(key, field string) (int64, error) {
	return s.ZRankCtx(s.ctx, key, field)
}

// ZRankCtx is the implementation of redis zrank command.
func (s *Redis) ZRankCtx(ctx context.Context, key, field string) (val int64, err error) {
	return s.client.ZRank(ctx, key, field).Result()
}

// ZRevRank is the implementation of redis zrevrank command.
func (s *Redis) ZRevRank(key, field string) (int64, error) {
	return s.ZRevRankCtx(s.ctx, key, field)
}

// ZRevRankCtx is the implementation of redis zrevrank command.
func (s *Redis) ZRevRankCtx(ctx context.Context, key, field string) (val int64, err error) {
	return s.client.ZRevRank(ctx, key, field).Result()
}

// ZRem is the implementation of redis zrem command.
func (s *Redis) ZRem(key string, values ...interface{}) (int64, error) {
	return s.ZRemCtx(s.ctx, key, values...)
}

// ZRemCtx is the implementation of redis zrem command.
func (s *Redis) ZRemCtx(ctx context.Context, key string, values ...interface{}) (val int64, err error) {
	return s.client.ZRem(ctx, key, values...).Result()
}

// ZRemRangeByScore is the implementation of redis zremrangebyscore command.
func (s *Redis) ZRemRangeByScore(key string, min, max string) (int64, error) {
	return s.ZRemRangeByScoreCtx(s.ctx, key, min, max)
}

// ZRemRangeByScoreCtx is the implementation of redis zremrangebyscore command.
func (s *Redis) ZRemRangeByScoreCtx(ctx context.Context, key string, min, max string) (
	val int64, err error) {
	return s.client.ZRemRangeByScore(ctx, key, min, max).Result()
}

// ZRemRangeByScoreInt64 is the implementation of redis zremrangebyscore command.
func (s *Redis) ZRemRangeByScoreInt64(key string, start, stop int64) (int64, error) {
	return s.ZRemRangeByScoreInt64Ctx(s.ctx, key, start, stop)
}

// ZRemRangeByScoreInt64Ctx is the implementation of redis zremrangebyscore command.
func (s *Redis) ZRemRangeByScoreInt64Ctx(ctx context.Context, key string, start, stop int64) (
	val int64, err error) {
	return s.client.ZRemRangeByScore(ctx, key, strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10)).Result()
}

// TODO ZRemRangeByScoreFloat

// ZRemRangeByRank is the implementation of redis zremrangebyrank command.
func (s *Redis) ZRemRangeByRank(key string, start, stop int64) (int64, error) {
	return s.ZRemRangeByRankCtx(s.ctx, key, start, stop)
}

// ZRemRangeByRankCtx is the implementation of redis zremrangebyrank command.
func (s *Redis) ZRemRangeByRankCtx(ctx context.Context, key string, start, stop int64) (
	val int64, err error) {
	return s.client.ZRemRangeByRank(ctx, key, start, stop).Result()
}

// TODO Zremrangebylex

// --------

// ZRange is the implementation of redis zrange command.
func (s *Redis) ZRange(key string, start, stop int64) ([]string, error) {
	return s.ZRangeCtx(s.ctx, key, start, stop)
}

// ZRangeCtx is the implementation of redis zrange command.
func (s *Redis) ZRangeCtx(ctx context.Context, key string, start, stop int64) (
	val []string, err error) {
	return s.client.ZRange(ctx, key, start, stop).Result()
}

// ZRangeByScore is the implementation of redis zrangebyscore command.
func (s *Redis) ZRangeByScore(key string, min, max string) ([]string, error) {
	return s.ZRangeByScoreCtx(s.ctx, key, min, max)
}

// ZRangeByScoreCtx is the implementation of redis zrangebyscore command.
func (s *Redis) ZRangeByScoreCtx(ctx context.Context, key string, min, max string) (
	val []string, err error) {
	val, err = s.client.ZRangeByScore(ctx, key, &red.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()
	return
}

// ZRangeByScoreAll is the implementation of redis zrangebyscore command.
func (s *Redis) ZRangeByScoreAll(key string) ([]string, error) {
	return s.ZRangeByScoreAllCtx(s.ctx, key)
}

// ZRangeByScoreAllCtx is the implementation of redis zrangebyscore command.
func (s *Redis) ZRangeByScoreAllCtx(ctx context.Context, key string) (
	val []string, err error) {
	val, err = s.client.ZRangeByScore(ctx, key, &red.ZRangeBy{
		Min: "-inf",
		Max: "+inf",
	}).Result()
	return
}

// ZRangeByScoreAndLimit is the implementation of redis zrangebyscore command.
func (s *Redis) ZRangeByScoreAndLimit(key string, min, max string, page, size int64) ([]string, error) {
	return s.ZRangeByScoreAndLimitCtx(s.ctx, key, min, max, page, size)
}

// ZRangeByScoreAndLimitCtx is the implementation of redis zrangebyscore command.
func (s *Redis) ZRangeByScoreAndLimitCtx(ctx context.Context, key string, min, max string, page, size int64) (
	val []string, err error) {
	val, err = s.client.ZRangeByScore(ctx, key, &red.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: page * size,
		Count:  size,
	}).Result()
	return
}

func (s *Redis) ZRangeByScoreInt64AndLimit(key string, min, max int64, page, size int64) (val []string, err error) {
	return s.ZRangeByScoreInt64AndLimitCtx(s.ctx, key, min, max, page, size)
}

func (s *Redis) ZRangeByScoreInt64AndLimitCtx(ctx context.Context, key string, min, max int64, page, size int64) (val []string, err error) {
	val, err = s.client.ZRangeByScore(ctx, key, &red.ZRangeBy{
		Min:    strconv.FormatInt(min, 10),
		Max:    strconv.FormatInt(max, 10),
		Offset: page * size,
		Count:  size,
	}).Result()
	return
}

// ZRangeByScoreAllAndLimit is the implementation of redis zrangebyscore command.
func (s *Redis) ZRangeByScoreAllAndLimit(key string, page, size int64) ([]string, error) {
	return s.ZRangeByScoreAllAndLimitCtx(s.ctx, key, page, size)
}

// ZRangeByScoreAllAndLimitCtx is the implementation of redis zrangebyscore command.
func (s *Redis) ZRangeByScoreAllAndLimitCtx(ctx context.Context, key string, page, size int64) (
	val []string, err error) {
	val, err = s.client.ZRangeByScore(ctx, key, &red.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: page * size,
		Count:  size,
	}).Result()
	return
}

// ZRangeWithScores is the implementation of redis zrange command with scores.
func (s *Redis) ZRangeWithScores(key string, start, stop int64) ([]Pair, error) {
	return s.ZRangeWithScoresCtx(s.ctx, key, start, stop)
}

// ZRangeWithScoresCtx is the implementation of redis zrange command with scores.
func (s *Redis) ZRangeWithScoresCtx(ctx context.Context, key string, start, stop int64) (
	val []Pair, err error) {
	v, err := s.client.ZRangeWithScores(ctx, key, start, stop).Result()
	val = toPairs(v)
	return
}

// ZRangeWithScoresFloat is the implementation of redis zrange command with scores by float64.
func (s *Redis) ZRangeWithScoresFloat(key string, start, stop int64) ([]FloatPair, error) {
	return s.ZRangeWithScoresFloatCtx(s.ctx, key, start, stop)
}

// ZRangeWithScoresFloatCtx is the implementation of redis zrange command with scores by float64.
func (s *Redis) ZRangeWithScoresFloatCtx(ctx context.Context, key string, start, stop int64) (
	val []FloatPair, err error) {
	v, err := s.client.ZRangeWithScores(ctx, key, start, stop).Result()
	val = toFloatPairs(v)
	return
}

// TODO Zrangebylex

// ZRangeByScoreWithScores is the implementation of redis zrangebyscore command with scores.
func (s *Redis) ZRangeByScoreWithScores(key string, min, max string) ([]Pair, error) {
	return s.ZRangeByScoreWithScoresCtx(s.ctx, key, min, max)
}

// ZRangeByScoreWithScoresCtx is the implementation of redis zrangebyscore command with scores.
func (s *Redis) ZRangeByScoreWithScoresCtx(ctx context.Context, key string, min, max string) (
	val []Pair, err error) {
	v, err := s.client.ZRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()
	val = toPairs(v)
	return
}

// ZRangeByScoreWithScoresAll is the implementation of redis zrangebyscore command with scores.
func (s *Redis) ZRangeByScoreWithScoresAll(key string) ([]Pair, error) {
	return s.ZRangeByScoreWithScoresAllCtx(s.ctx, key)
}

// ZRangeByScoreWithScoresAllCtx is the implementation of redis zrangebyscore command with scores.
func (s *Redis) ZRangeByScoreWithScoresAllCtx(ctx context.Context, key string) (
	val []Pair, err error) {
	v, err := s.client.ZRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min: "-inf",
		Max: "+inf",
	}).Result()
	val = toPairs(v)
	return
}

// ZRangeByScoreWithScoresInt64 is the implementation of redis zrangebyscore command with scores.
func (s *Redis) ZRangeByScoreWithScoresInt64(key string, start, stop int64) ([]Pair, error) {
	return s.ZRangeByScoreWithScoresInt64Ctx(s.ctx, key, start, stop)
}

// ZRangeByScoreWithScoresInt64Ctx is the implementation of redis zrangebyscore command with scores.
func (s *Redis) ZRangeByScoreWithScoresInt64Ctx(ctx context.Context, key string, start, stop int64) (
	val []Pair, err error) {
	v, err := s.client.ZRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min: strconv.FormatInt(start, 10),
		Max: strconv.FormatInt(stop, 10),
	}).Result()
	val = toPairs(v)
	return
}

// ZRangeByScoreWithScoresFloat is the implementation of redis zrangebyscore command with scores by float.
func (s *Redis) ZRangeByScoreWithScoresFloat(key string, start, stop float64) ([]FloatPair, error) {
	return s.ZRangeByScoreWithScoresFloatCtx(s.ctx, key, start, stop)
}

// ZRangeByScoreWithScoresFloatCtx is the implementation of redis zrangebyscore command with scores by float.
func (s *Redis) ZRangeByScoreWithScoresFloatCtx(ctx context.Context, key string, start, stop float64) (
	val []FloatPair, err error) {
	v, err := s.client.ZRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min: strconv.FormatFloat(start, 'f', -1, 64),
		Max: strconv.FormatFloat(stop, 'f', -1, 64),
	}).Result()
	val = toFloatPairs(v)
	return
}

// ZRangeByScoreWithScoresAndLimit is the implementation of redis zrangebyscore command
// with scores and limit.
func (s *Redis) ZRangeByScoreWithScoresAndLimit(key string, min, max string, page, size int64) ([]Pair, error) {
	return s.ZRangeByScoreWithScoresAndLimitCtx(s.ctx, key, min, max, page, size)
}

// ZRangeByScoreWithScoresAndLimitCtx is the implementation of redis zrangebyscore command
// with scores and limit.
func (s *Redis) ZRangeByScoreWithScoresAndLimitCtx(ctx context.Context, key string, min, max string, page, size int64) (val []Pair, err error) {
	v, err := s.client.ZRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: page * size,
		Count:  size,
	}).Result()
	val = toPairs(v)
	return
}

// ZRangeByScoreWithScoresAllAndLimit is the implementation of redis zrangebyscore command
// with scores and limit.
func (s *Redis) ZRangeByScoreWithScoresAllAndLimit(key string, page, size int64) ([]Pair, error) {
	return s.ZRangeByScoreWithScoresAllAndLimitCtx(s.ctx, key, page, size)
}

// ZRangeByScoreWithScoresAllAndLimitCtx is the implementation of redis zrangebyscore command
// with scores and limit.
func (s *Redis) ZRangeByScoreWithScoresAllAndLimitCtx(ctx context.Context, key string, page, size int64) (val []Pair, err error) {
	v, err := s.client.ZRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: page * size,
		Count:  size,
	}).Result()
	val = toPairs(v)
	return
}

// ZRangeByScoreWithScoresInt64AndLimit is the implementation of redis zrangebyscore command
// with scores and limit.
func (s *Redis) ZRangeByScoreWithScoresInt64AndLimit(key string, start, stop int64,
	page, size int64) ([]Pair, error) {
	return s.ZRangeByScoreWithScoresInt64AndLimitCtx(s.ctx, key, start, stop, page, size)
}

// ZRangeByScoreWithScoresInt64AndLimitCtx is the implementation of redis zrangebyscore command
// with scores and limit.
func (s *Redis) ZRangeByScoreWithScoresInt64AndLimitCtx(ctx context.Context, key string, start,
	stop int64, page, size int64) (val []Pair, err error) {
	v, err := s.client.ZRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min:    strconv.FormatInt(start, 10),
		Max:    strconv.FormatInt(stop, 10),
		Offset: page * size,
		Count:  size,
	}).Result()
	val = toPairs(v)
	return
}

// ZRangeByScoreWithScoresFloatAndLimit is the implementation of redis zrangebyscore command
// with scores by float and limit.
func (s *Redis) ZRangeByScoreWithScoresFloatAndLimit(key string, start, stop float64,
	page, size int64) ([]FloatPair, error) {
	return s.ZRangeByScoreWithScoresFloatAndLimitCtx(s.ctx, key, start, stop, page, size)
}

// ZRangeByScoreWithScoresFloatAndLimitCtx is the implementation of redis zrangebyscore command
// with scores by float and limit.
func (s *Redis) ZRangeByScoreWithScoresFloatAndLimitCtx(ctx context.Context, key string, start,
	stop float64, page, size int64) (val []FloatPair, err error) {
	v, err := s.client.ZRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min:    strconv.FormatFloat(start, 'f', -1, 64),
		Max:    strconv.FormatFloat(stop, 'f', -1, 64),
		Offset: page * size,
		Count:  size,
	}).Result()
	val = toFloatPairs(v)
	return
}

// ZRangeByScoreWithScoresInt64AllLimit is the implementation of redis ZRangeByScoreWithScores command
// with scores and limit.
func (s *Redis) ZRangeByScoreWithScoresInt64AllLimit(key string, start, stop int64,
	page, size int64) ([]Pair, error) {
	return s.ZRangeByScoreWithScoresInt64AllLimitCtx(s.ctx, key, start, stop, page, size)
}

// ZRangeByScoreWithScoresInt64AllLimitCtx is the implementation of redis ZRangeByScoreWithScores command
// with scores and limit.
func (s *Redis) ZRangeByScoreWithScoresInt64AllLimitCtx(ctx context.Context, key string, start, stop int64,
	page, size int64) (val []Pair, err error) {

	v, err := s.client.ZRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min:    strconv.FormatInt(start, 10),
		Max:    strconv.FormatInt(stop, 10),
		Offset: page * size,
		Count:  size,
	}).Result()
	val = toPairs(v)
	return
}

// ZRangeByScoreWithScoresFloatAllLimit is the implementation of redis ZRangeByScoreWithScores command
// with scores by float and limit.
func (s *Redis) ZRangeByScoreWithScoresFloatAllLimit(key string, start, stop float64, page, size int64) ([]FloatPair, error) {
	return s.ZRangeByScoreWithScoresFloatAllLimitCtx(s.ctx, key, start, stop, page, size)
}

// ZRangeByScoreWithScoresFloatAllLimitCtx is the implementation of redis ZRangeByScoreWithScores command
// with scores by float and limit.
func (s *Redis) ZRangeByScoreWithScoresFloatAllLimitCtx(ctx context.Context, key string, start, stop float64, page, size int64) (val []FloatPair, err error) {
	v, err := s.client.ZRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min:    strconv.FormatFloat(start, 'f', -1, 64),
		Max:    strconv.FormatFloat(stop, 'f', -1, 64),
		Offset: page * size,
		Count:  size,
	}).Result()
	val = toFloatPairs(v)
	return
}

// ------------

// ZRevRange is the implementation of redis zrevrange command.
func (s *Redis) ZRevRange(key string, start, stop int64) ([]string, error) {
	return s.ZRevRangeCtx(s.ctx, key, start, stop)
}

// ZRevRangeCtx is the implementation of redis zrevrange command.
func (s *Redis) ZRevRangeCtx(ctx context.Context, key string, start, stop int64) (
	val []string, err error) {
	return s.client.ZRevRange(ctx, key, start, stop).Result()
}

// ZRevRangeByScore is the implementation of redis zrevrangebyscore command.
func (s *Redis) ZRevRangeByScore(key string, min, max string) ([]string, error) {
	return s.ZRevRangeByScoreCtx(s.ctx, key, min, max)
}

// ZRevRangeByScoreCtx is the implementation of redis zrevrangebyscore command.
func (s *Redis) ZRevRangeByScoreCtx(ctx context.Context, key string, min, max string) (
	val []string, err error) {
	val, err = s.client.ZRevRangeByScore(ctx, key, &red.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()
	return
}

// ZRevRangeByScoreAll is the implementation of redis zrevrangebyscore command.
func (s *Redis) ZRevRangeByScoreAll(key string) ([]string, error) {
	return s.ZRevRangeByScoreAllCtx(s.ctx, key)
}

// ZRevRangeByScoreAllCtx is the implementation of redis zrevrangebyscore command.
func (s *Redis) ZRevRangeByScoreAllCtx(ctx context.Context, key string) (
	val []string, err error) {
	val, err = s.client.ZRevRangeByScore(ctx, key, &red.ZRangeBy{
		Min: "-inf",
		Max: "+inf",
	}).Result()
	return
}

// ZRevRangeByScoreAndLimit is the implementation of redis zrevrangebyscore command.
func (s *Redis) ZRevRangeByScoreAndLimit(key string, min, max string, page, size int64) ([]string, error) {
	return s.ZRevRangeByScoreAndLimitCtx(s.ctx, key, min, max, page, size)
}

// ZRevRangeByScoreAndLimitCtx is the implementation of redis zrevrangebyscore command.
func (s *Redis) ZRevRangeByScoreAndLimitCtx(ctx context.Context, key string, min, max string, page, size int64) (
	val []string, err error) {
	val, err = s.client.ZRevRangeByScore(ctx, key, &red.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: page * size,
		Count:  size,
	}).Result()
	return
}

// ZRevRangeByScoreAllAndLimit is the implementation of redis zrevrangebyscore command.
func (s *Redis) ZRevRangeByScoreAllAndLimit(key string, page, size int64) ([]string, error) {
	return s.ZRevRangeByScoreAllAndLimitCtx(s.ctx, key, page, size)
}

// ZRevRangeByScoreAllAndLimitCtx is the implementation of redis zrevrangebyscore command.
func (s *Redis) ZRevRangeByScoreAllAndLimitCtx(ctx context.Context, key string, page, size int64) (
	val []string, err error) {
	val, err = s.client.ZRangeByScore(ctx, key, &red.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: page * size,
		Count:  size,
	}).Result()
	return
}

// ZRevRangeByScoreWithScores is the implementation of redis zrevrangebyscore command with scores.
func (s *Redis) ZRevRangeByScoreWithScores(key string, min, max string) ([]Pair, error) {
	return s.ZRevRangeByScoreWithScoresCtx(s.ctx, key, min, max)
}

// ZRevRangeByScoreWithScoresCtx is the implementation of redis zrevrangebyscore command with scores.
func (s *Redis) ZRevRangeByScoreWithScoresCtx(ctx context.Context, key string, min, max string) (
	val []Pair, err error) {
	v, err := s.client.ZRevRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()
	val = toPairs(v)
	return
}

// ZRevRangeByScoreWithScoresAll is the implementation of redis zrevrangebyscore command with scores.
func (s *Redis) ZRevRangeByScoreWithScoresAll(key string) ([]Pair, error) {
	return s.ZRevRangeByScoreWithScoresAllCtx(s.ctx, key)
}

// ZRevRangeByScoreWithScoresAllCtx is the implementation of redis zrevrangebyscore command with scores.
func (s *Redis) ZRevRangeByScoreWithScoresAllCtx(ctx context.Context, key string) (
	val []Pair, err error) {
	v, err := s.client.ZRevRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min: "-inf",
		Max: "+inf",
	}).Result()
	val = toPairs(v)
	return
}

// ZRevRangeByScoreWithScoresInt64 is the implementation of redis zrevrangebyscore command with scores.
func (s *Redis) ZRevRangeByScoreWithScoresInt64(key string, start, stop int64) ([]Pair, error) {
	return s.ZRevRangeByScoreWithScoresInt64Ctx(s.ctx, key, start, stop)
}

// ZRevRangeByScoreWithScoresInt64Ctx is the implementation of redis zrevrangebyscore command with scores.
func (s *Redis) ZRevRangeByScoreWithScoresInt64Ctx(ctx context.Context, key string, start, stop int64) (
	val []Pair, err error) {
	v, err := s.client.ZRevRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min: strconv.FormatInt(start, 10),
		Max: strconv.FormatInt(stop, 10),
	}).Result()
	val = toPairs(v)
	return
}

// ZRevRangeByScoreWithScoresFloat is the implementation of redis zrevrangebyscore command with scores by float.
func (s *Redis) ZRevRangeByScoreWithScoresFloat(key string, start, stop float64) (
	[]FloatPair, error) {
	return s.ZRevRangeByScoreWithScoresFloatCtx(s.ctx, key, start, stop)
}

// ZRevRangeByScoreWithScoresFloatCtx is the implementation of redis zrevrangebyscore command with scores by float.
func (s *Redis) ZRevRangeByScoreWithScoresFloatCtx(ctx context.Context, key string,
	start, stop float64) (val []FloatPair, err error) {
	v, err := s.client.ZRevRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min: strconv.FormatFloat(start, 'f', -1, 64),
		Max: strconv.FormatFloat(stop, 'f', -1, 64),
	}).Result()
	val = toFloatPairs(v)
	return
}

// ZRevRangeByScoreWithScoresAndLimit is the implementation of redis zrevrangebyscore command
// with scores and limit.
func (s *Redis) ZRevRangeByScoreWithScoresAndLimit(key string, min, max string,
	page, size int64) ([]Pair, error) {
	return s.ZRevRangeByScoreWithScoresAndLimitCtx(s.ctx, key, min, max, page, size)
}

// ZRevRangeByScoreWithScoresAndLimitCtx is the implementation of redis zrevrangebyscore command
// with scores and limit.
func (s *Redis) ZRevRangeByScoreWithScoresAndLimitCtx(ctx context.Context, key string,
	min, max string, page, size int64) (val []Pair, err error) {

	v, err := s.client.ZRevRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: page * size,
		Count:  size,
	}).Result()
	val = toPairs(v)
	return
}

// ZRevRangeByScoreWithScoresAllAndLimit is the implementation of redis zrevrangebyscore command
// with scores and limit.
func (s *Redis) ZRevRangeByScoreWithScoresAllAndLimit(key string,
	page, size int64) ([]Pair, error) {
	return s.ZRevRangeByScoreWithScoresAllAndLimitCtx(s.ctx, key, page, size)
}

// ZRevRangeByScoreWithScoresAllAndLimitCtx is the implementation of redis zrevrangebyscore command
// with scores and limit.
func (s *Redis) ZRevRangeByScoreWithScoresAllAndLimitCtx(ctx context.Context, key string,
	page, size int64) (val []Pair, err error) {

	v, err := s.client.ZRevRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: page * size,
		Count:  size,
	}).Result()
	val = toPairs(v)
	return
}

// ZRevRangeByScoreWithScoresInt64AndLimit is the implementation of redis zrevrangebyscore command
// with scores and limit.
func (s *Redis) ZRevRangeByScoreWithScoresInt64AndLimit(key string, start, stop int64,
	page, size int64) ([]Pair, error) {
	return s.ZRevRangeByScoreWithScoresInt64AndLimitCtx(s.ctx, key, start, stop, page, size)
}

// ZRevRangeByScoreWithScoresInt64AndLimitCtx is the implementation of redis zrevrangebyscore command
// with scores and limit.
func (s *Redis) ZRevRangeByScoreWithScoresInt64AndLimitCtx(ctx context.Context, key string,
	start, stop int64, page, size int64) (val []Pair, err error) {

	v, err := s.client.ZRevRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min:    strconv.FormatInt(start, 10),
		Max:    strconv.FormatInt(stop, 10),
		Offset: page * size,
		Count:  size,
	}).Result()
	val = toPairs(v)
	return
}

// ZRevRangeByScoreWithScoresFloatAndLimit is the implementation of redis zrevrangebyscore command
// with scores by float and limit.
func (s *Redis) ZRevRangeByScoreWithScoresFloatAndLimit(key string, start, stop float64,
	page, size int64) ([]FloatPair, error) {
	return s.ZRevRangeByScoreWithScoresFloatAndLimitCtx(s.ctx,
		key, start, stop, page, size)
}

// ZRevRangeByScoreWithScoresFloatAndLimitCtx is the implementation of redis zrevrangebyscore command
// with scores by float and limit.
func (s *Redis) ZRevRangeByScoreWithScoresFloatAndLimitCtx(ctx context.Context, key string,
	start, stop float64, page, size int64) (val []FloatPair, err error) {

	v, err := s.client.ZRevRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min:    strconv.FormatFloat(start, 'f', -1, 64),
		Max:    strconv.FormatFloat(stop, 'f', -1, 64),
		Offset: page * size,
		Count:  size,
	}).Result()
	val = toFloatPairs(v)
	return
}

// ZRevRangeByScoreWithScoresInt64AllLimit is the implementation of redis zrevrangebyscore command
// with scores and limit.
func (s *Redis) ZRevRangeByScoreWithScoresInt64AllLimit(key string, page, size int64) ([]Pair, error) {
	return s.ZRevRangeByScoreWithScoresInt64AllLimitCtx(s.ctx, key, page, size)
}

// ZRevRangeByScoreWithScoresInt64AllLimitCtx is the implementation of redis zrevrangebyscore command
// with scores and limit.
func (s *Redis) ZRevRangeByScoreWithScoresInt64AllLimitCtx(ctx context.Context, key string,
	page, size int64) (val []Pair, err error) {
	v, err := s.client.ZRevRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: page * size,
		Count:  size,
	}).Result()
	val = toPairs(v)
	return
}

// ZRevRangeByScoreWithScoresFloatAllLimit is the implementation of redis zrevrangebyscore command
// with scores by float and limit.
func (s *Redis) ZRevRangeByScoreWithScoresFloatAllLimit(key string, start, stop float64,
	page, size int64) ([]FloatPair, error) {
	return s.ZRevRangeByScoreWithScoresFloatAllLimitCtx(s.ctx,
		key, page, size)
}

// ZRevRangeByScoreWithScoresFloatAllLimitCtx is the implementation of redis zrevrangebyscore command
// with scores by float and limit.
func (s *Redis) ZRevRangeByScoreWithScoresFloatAllLimitCtx(ctx context.Context, key string,
	page, size int64) (val []FloatPair, err error) {

	v, err := s.client.ZRevRangeByScoreWithScores(ctx, key, &red.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: page * size,
		Count:  size,
	}).Result()
	val = toFloatPairs(v)
	return
}

// ---------

// ZUnionStore is the implementation of redis zunionstore command.
func (s *Redis) ZUnionStore(dest string, store *ZStore) (int64, error) {
	return s.ZUnionStoreCtx(s.ctx, dest, store)
}

// ZUnionStoreCtx is the implementation of redis zunionstore command.
func (s *Redis) ZUnionStoreCtx(ctx context.Context, dest string, store *ZStore) (
	val int64, err error) {
	return s.client.ZUnionStore(ctx, dest, store).Result()
}

// ---------

func (s *Redis) ZPopMin(key string, count int64) (val []Pair, err error) {
	return s.ZPopMinCtx(s.ctx, key, count)
}

func (s *Redis) ZPopMinCtx(ctx context.Context, key string, count int64) (val []Pair, err error) {
	v, err := s.client.ZPopMin(ctx, key, count).Result()
	val = toPairs(v)
	return
}

func (s *Redis) ZPopMax(key string, count int64) (val []Pair, err error) {
	return s.ZPopMaxCtx(s.ctx, key, count)
}

func (s *Redis) ZPopMaxCtx(ctx context.Context, key string, count int64) (val []Pair, err error) {
	v, err := s.client.ZPopMax(ctx, key, count).Result()
	val = toPairs(v)
	return
}

//func (s *Redis) ZMPop(key string, count int64) (val []Pair, err error) {
//	return s.ZPopMinCtx(s.ctx, key, count)
//}
//
//func (s *Redis) ZMPopCtx(ctx context.Context, key string, count int64) (val []Pair, err error) {
//
//	return
//}

func (s *Redis) ZMScore(key string, members ...string) (val []float64, err error) {
	return s.ZMScoreCtx(s.ctx, key, members...)
}

func (s *Redis) ZMScoreCtx(ctx context.Context, key string, members ...string) (val []float64, err error) {
	val, err = s.client.ZMScore(ctx, key, members...).Result()
	return
}

func (s *Redis) ZRandMember(key string, count int) (val []string, err error) {
	return s.ZRandMemberCtx(s.ctx, key, count)
}

func (s *Redis) ZRandMemberCtx(ctx context.Context, key string, count int) (val []string, err error) {
	val, err = s.client.ZRandMember(ctx, key, count).Result()
	return
}

func (s *Redis) ZRandMemberWithScoresCtx(ctx context.Context, key string, count int) (val []Pair, err error) {
	v, err := s.client.ZRandMemberWithScores(ctx, key, count).Result()
	val = toPairs(v)
	return
}
