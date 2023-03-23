# rose-redis

## Install

```
go get -u github.com/leafney/rose-redis
```

## Use

```go
	client,err := rredis.NewRedis("127.0.0.1:6379")
	if err != nil{
		fmt.Println(err)
		return
	}
	
	client.Set("hello", "world")
```
