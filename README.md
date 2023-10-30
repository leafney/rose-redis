# rose-redis

## Introduce

Derived from the built-in `redis` methods in `go-zero` project, we have enhanced and optimized operations on `redis`.

## Install

```
go get -u github.com/leafney/rose-redis
```

## Use

```go
    // client,err := rredis.NewRedis("127.0.0.1:6379", nil)
    client, err := NewRedis("127.0.0.1:6379", &Option{
        Db:   3,
        Type: TypeNode,
    })
	if err != nil{
		fmt.Println(err)
		return
	}
	
	client.Set("hello", "world")
```

## Methods

### zset

- ZRange
- ZRangeCtx
-
- ZRangeByScore
- ZRangeByScoreCtx
- ZRangeByScoreAll
- ZRangeByScoreAllCtx
- ZRangeByScoreAndLimit
- ZRangeByScoreAndLimitCtx
- ZRangeByScoreAllAndLimit
- ZRangeByScoreAllAndLimitCtx
-
- ZRangeWithScores
- ZRangeWithScoresCtx
- ZRangeWithScoresFloat
- ZRangeWithScoresFloatCtx
-
- ZRangeByScoreWithScores
- ZRangeByScoreWithScoresCtx
- ZRangeByScoreWithScoresAll
- ZRangeByScoreWithScoresAllCtx
- ZRangeByScoreWithScoresInt64
- ZRangeByScoreWithScoresInt64Ctx
- ZRangeByScoreWithScoresFloat
- ZRangeByScoreWithScoresFloatCtx
-
- ZRangeByScoreWithScoresAndLimit
- ZRangeByScoreWithScoresAndLimitCtx
- ZRangeByScoreWithScoresAllAndLimit
- ZRangeByScoreWithScoresAllAndLimitCtx
-
- ZRangeByScoreWithScoresInt64AndLimit
- ZRangeByScoreWithScoresInt64AndLimitCtx
- ZRangeByScoreWithScoresFloatAndLimit
- ZRangeByScoreWithScoresFloatAndLimitCtx
-
- ZRangeByScoreWithScoresInt64AllLimit
- ZRangeByScoreWithScoresInt64AllLimitCtx
- ZRangeByScoreWithScoresFloatAllLimit
- ZRangeByScoreWithScoresFloatAllLimitCtx
-
- ZRevRange
- ZRevRangeCtx
-
- ZRevRangeByScore
- ZRevRangeByScoreCtx
- ZRevRangeByScoreAll
- ZRevRangeByScoreAllCtx
- ZRevRangeByScoreAndLimit
- ZRevRangeByScoreAndLimitCtx
- ZRevRangeByScoreAllAndLimit
- ZRevRangeByScoreAllAndLimitCtx
-
- ZRevRangeByScoreWithScores
- ZRevRangeByScoreWithScoresCtx
- ZRevRangeByScoreWithScoresAll
- ZRevRangeByScoreWithScoresAllCtx
- ZRevRangeByScoreWithScoresInt64
- ZRevRangeByScoreWithScoresInt64Ctx
- ZRevRangeByScoreWithScoresFloat
- ZRevRangeByScoreWithScoresFloatCtx
-
- ZRevRangeByScoreWithScoresAndLimit
- ZRevRangeByScoreWithScoresAndLimitCtx
- ZRevRangeByScoreWithScoresAllAndLimit
- ZRevRangeByScoreWithScoresAllAndLimitCtx
-
- ZRevRangeByScoreWithScoresInt64AndLimit
- ZRevRangeByScoreWithScoresInt64AndLimitCtx
- ZRevRangeByScoreWithScoresFloatAndLimit
- ZRevRangeByScoreWithScoresFloatAndLimitCtx
-
- ZRevRangeByScoreWithScoresInt64AllLimit
- ZRevRangeByScoreWithScoresInt64AllLimitCtx
- ZRevRangeByScoreWithScoresFloatAllLimit
- ZRevRangeByScoreWithScoresFloatAllLimitCtx

----
