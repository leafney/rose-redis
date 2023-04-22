package rredis

import (
	"testing"
)

func TestNewRedis(t *testing.T) {

	//client, err := NewRedis("127.0.0.1:6379", nil)
	client, err := NewRedis("127.0.0.1:6379", &Option{
		Db:   3,
		Type: TypeNode,
	})

	if err != nil {
		t.Error(err)
		return
	}
	defer client.Close()

	client.Set("abcdef", "hello22")

}
