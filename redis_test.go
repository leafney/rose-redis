package rredis

import (
	"testing"
)

func TestNewRedis(t *testing.T) {

	//client := NewClient("", nil)

	//client, err := NewRedis("127.0.0.1:6379", nil)

	client, err := NewRedis("127.0.0.1:6379", &Option{
		DB:   3,
		Type: TypeNode,
	})

	if err != nil {
		t.Error(err)
		return
	}
	defer client.Close()

	//client.Set("abcdef", "hello")

	//client.GetWithCache("",10,100, func() (interface{}, error) {
	//
	//})

}
