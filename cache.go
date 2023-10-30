/**
 * @Author:      leafney
 * @GitHub:      https://github.com/leafney
 * @Project:     rose-redis
 * @Date:        2023-08-08 13:07
 * @Description:
 */

package rredis

import "math/rand"

func (s *Redis) GetWithCache(key string, minExpire, maxExpire int64, queryFunc func() (interface{}, error)) (interface{}, error) {
	result, err := s.Get(key)
	if err == nil {
		return result, nil
	}

	data, err := queryFunc()
	if err != nil {
		return nil, err
	}

	expire := rand.Int63n(maxExpire-minExpire+1) + minExpire

	err = s.SetEx(key, data, expire)
	if err != nil {
		return nil, err
	}

	return data, nil
}
