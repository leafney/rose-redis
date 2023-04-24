package rredis

import "context"

// Pipelined lets fn execute pipelined commands.
func (s *Redis) Pipelined(fn func(Pipeliner) error) error {
	return s.PipelinedCtx(s.ctx, fn)
}

// PipelinedCtx lets fn execute pipelined commands.
// Results need to be retrieved by calling Pipeline.Exec()
func (s *Redis) PipelinedCtx(ctx context.Context, fn func(Pipeliner) error) error {
	_, err := s.client.Pipelined(ctx, fn)
	return err
}
