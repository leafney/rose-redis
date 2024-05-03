/**
 * @Author:      leafney
 * @GitHub:      https://github.com/leafney
 * @Project:     rose-redis
 * @Date:        2024-05-03 12:14
 * @Description:
 */

package queue

import (
	"context"
	"fmt"
	rredis "github.com/leafney/rose-redis"
	"testing"
	"time"
)

func TestNewSQueue(t *testing.T) {

	client, _ := rredis.NewRedis("127.0.0.1:6379", &rredis.Option{
		Db:   3,
		Type: rredis.TypeNode,
	})

	ctx := context.Background()

	Q := NewSQueue(client)

	t.Log(Q.Consume(ctx, "test", "aaa", "1234", 1, consumeMsg))
	//t.Log(Q.Consume(ctx, "test", "aaa", "5678", 1, consumeMsg))

	for i := 0; i < 10; i++ {
		msg := map[string]interface{}{
			"title": i,
		}
		t.Log(Q.Publish(ctx, "test", msg))
	}

	//t.Log(Q.SetMaxCount(ctx, "test", 10))
	t.Log("消息发送完毕")

	//for {
	//	t.Logf("count %v", Q.Count(ctx, "test"))
	//	time.Sleep(2 * time.Second)
	//}

	select {}
}

func consumeMsg(info *MsgInfo, msg map[string]interface{}) error {

	fmt.Println(info)
	fmt.Println("接收到 ", msg)
	time.Sleep(5 * time.Second)
	return nil
}
