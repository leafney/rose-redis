package rredis

import "context"

// PFAdd is the implementation of redis pfadd command.
func (s *Redis) PFAdd(key string, values ...interface{}) (bool, error) {
	return s.PFAddCtx(s.ctx, key, values...)
}

// PFAddCtx is the implementation of redis pfadd command.
func (s *Redis) PFAddCtx(ctx context.Context, key string, values ...interface{}) (val bool, err error) {
	v, err := s.client.PFAdd(ctx, key, values...).Result()
	return v >= 1, err
}

// PFCount is the implementation of redis pfcount command.
func (s *Redis) PFCount(key string) (int64, error) {
	return s.PFCountCtx(s.ctx, key)
}

// PFCountCtx is the implementation of redis pfcount command.
func (s *Redis) PFCountCtx(ctx context.Context, key string) (val int64, err error) {
	return s.client.PFCount(ctx, key).Result()
}

// PFMerge is the implementation of redis pfmerge command.
func (s *Redis) PFMerge(dest string, keys ...string) error {
	return s.PFMergeCtx(s.ctx, dest, keys...)
}

// PFMergeCtx is the implementation of redis pfmerge command.
func (s *Redis) PFMergeCtx(ctx context.Context, dest string, keys ...string) error {
	_, err := s.client.PFMerge(ctx, dest, keys...).Result()
	return err
}
