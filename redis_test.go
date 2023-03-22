package rredis

import (
	"testing"
)

func TestNewRedis(t *testing.T) {

	client, err := NewRedis("127.0.0.1:6379", func(r *Redis) {
		r.Db = 3
		r.Type = NodeType
	})
	if err != nil {
		t.Error(err)
		return
	}

	client.Set("abcdef", "hello22")

}
