package rredis

import "context"

// GeoAdd is the implementation of redis geoadd command.
func (s *Redis) GeoAdd(key string, geoLocation ...*GeoLocation) (int64, error) {
	return s.GeoAddCtx(s.ctx, key, geoLocation...)
}

// GeoAddCtx is the implementation of redis geoadd command.
func (s *Redis) GeoAddCtx(ctx context.Context, key string, geoLocation ...*GeoLocation) (
	val int64, err error) {
	return s.client.GeoAdd(ctx, key, geoLocation...).Result()
}

// GeoDist is the implementation of redis geodist command.
func (s *Redis) GeoDist(key, member1, member2, unit string) (float64, error) {
	return s.GeoDistCtx(s.ctx, key, member1, member2, unit)
}

// GeoDistCtx is the implementation of redis geodist command.
func (s *Redis) GeoDistCtx(ctx context.Context, key, member1, member2, unit string) (
	val float64, err error) {
	return s.client.GeoDist(ctx, key, member1, member2, unit).Result()
}

// GeoHash is the implementation of redis geohash command.
func (s *Redis) GeoHash(key string, members ...string) ([]string, error) {
	return s.GeoHashCtx(s.ctx, key, members...)
}

// GeoHashCtx is the implementation of redis geohash command.
func (s *Redis) GeoHashCtx(ctx context.Context, key string, members ...string) (
	val []string, err error) {
	return s.client.GeoHash(ctx, key, members...).Result()
}

// GeoRadius is the implementation of redis georadius command.
func (s *Redis) GeoRadius(key string, longitude, latitude float64, query *GeoRadiusQuery) (
	[]GeoLocation, error) {
	return s.GeoRadiusCtx(s.ctx, key, longitude, latitude, query)
}

// GeoRadiusCtx is the implementation of redis georadius command.
func (s *Redis) GeoRadiusCtx(ctx context.Context, key string, longitude, latitude float64,
	query *GeoRadiusQuery) (val []GeoLocation, err error) {
	return s.client.GeoRadius(ctx, key, longitude, latitude, query).Result()
}

// GeoRadiusByMember is the implementation of redis georadiusbymember command.
func (s *Redis) GeoRadiusByMember(key, member string, query *GeoRadiusQuery) ([]GeoLocation, error) {
	return s.GeoRadiusByMemberCtx(s.ctx, key, member, query)
}

// GeoRadiusByMemberCtx is the implementation of redis georadiusbymember command.
func (s *Redis) GeoRadiusByMemberCtx(ctx context.Context, key, member string,
	query *GeoRadiusQuery) (val []GeoLocation, err error) {
	return s.client.GeoRadiusByMember(ctx, key, member, query).Result()
}

// GeoPos is the implementation of redis geopos command.
func (s *Redis) GeoPos(key string, members ...string) ([]*GeoPos, error) {
	return s.GeoPosCtx(s.ctx, key, members...)
}

// GeoPosCtx is the implementation of redis geopos command.
func (s *Redis) GeoPosCtx(ctx context.Context, key string, members ...string) (
	val []*GeoPos, err error) {
	return s.client.GeoPos(ctx, key, members...).Result()
}
