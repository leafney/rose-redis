package rredis

import "context"

// Eval is the implementation of redis eval command.
func (s *Redis) Eval(script string, keys []string, args ...interface{}) (interface{}, error) {
	return s.EvalCtx(s.ctx, script, keys, args...)
}

// EvalCtx is the implementation of redis eval command.
func (s *Redis) EvalCtx(ctx context.Context, script string, keys []string,
	args ...interface{}) (val interface{}, err error) {
	return s.client.Eval(ctx, script, keys, args...).Result()
}

// EvalSha is the implementation of redis evalsha command.
func (s *Redis) EvalSha(sha string, keys []string, args ...interface{}) (interface{}, error) {
	return s.EvalShaCtx(s.ctx, sha, keys, args...)
}

// EvalShaCtx is the implementation of redis evalsha command.
func (s *Redis) EvalShaCtx(ctx context.Context, sha string, keys []string,
	args ...interface{}) (val interface{}, err error) {
	return s.client.EvalSha(ctx, sha, keys, args...).Result()
}

// ScriptLoad is the implementation of redis script load command.
func (s *Redis) ScriptLoad(script string) (string, error) {
	return s.ScriptLoadCtx(s.ctx, script)
}

// ScriptLoadCtx is the implementation of redis script load command.
func (s *Redis) ScriptLoadCtx(ctx context.Context, script string) (string, error) {
	return s.client.ScriptLoad(ctx, script).Result()
}
