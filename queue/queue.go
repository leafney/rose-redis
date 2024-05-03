/**
 * @Author:      leafney
 * @GitHub:      https://github.com/leafney
 * @Project:     rose-redis
 * @Date:        2024-05-03 11:18
 * @Description:
 */

package queue

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	rredis "github.com/leafney/rose-redis"
	"strings"
)

type SQueue struct {
	client *rredis.Redis
}

func NewSQueue(c *rredis.Redis) *SQueue {
	return &SQueue{
		client: c,
	}
}

func (s *SQueue) Publish(ctx context.Context, topic string, msg map[string]interface{}) error {
	_, err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: topic,
		ID:     "*",
		Values: msg,
	})

	return err
}

func (s *SQueue) SetMaxCount(ctx context.Context, topic string, max int64) error {
	return s.client.XTrimMaxLenApprox(ctx, topic, max, 0)
}

func (s *SQueue) Count(ctx context.Context, topic string) int64 {
	res, err := s.client.XLen(ctx, topic)
	fmt.Println(res, err)
	if err != nil {
		return 0
	}
	return res
}

//func (s *SQueue) RemainCount(ctx context.Context, topic, group string) int64 {
//	res, err := s.client.XPending(ctx, topic, group)
//	if err != nil {
//		return 0
//	}
//	return res.Count
//}

type MsgInfo struct {
	Topic    string
	Group    string
	Consumer string
	MsgId    string
}

type ConsumeMsgHandler func(info *MsgInfo, msg map[string]interface{}) error

func (s *SQueue) Consume(ctx context.Context, topic, group, consumer string, batchSize int, handler ConsumeMsgHandler) error {
	// start 用于创建消费者组的时候指定起始消费ID，0表示从头开始消费，$表示从最后一条消息开始消费
	err := s.client.XGroupCreateMkStream(ctx, topic, group, "0")
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return err
	}

	go func() {
		for {
			if err := s.consume(ctx, topic, group, consumer, ">", batchSize, handler); err != nil {
				return
			}

			//if err := s.consume(ctx, topic, group, consumer, "0", batchSize, handler); err != nil {
			//	return
			//} else {
			//	fmt.Println("接收到消息 222")
			//}
		}
	}()

	return nil
}

func (s *SQueue) consume(ctx context.Context, topic, group, consumer, id string, batchSize int, h ConsumeMsgHandler) error {
	res, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{topic, id},
		Count:    int64(batchSize),
		Block:    0,
	})
	if err != nil {
		return err
	}

	for _, rs := range res {
		for _, msg := range rs.Messages {
			err := h(&MsgInfo{
				Topic:    topic,
				Group:    group,
				Consumer: consumer,
				MsgId:    msg.ID,
			}, msg.Values)
			if err == nil {
				if err := s.client.XAck(ctx, topic, group, msg.ID); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
