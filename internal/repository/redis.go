package repository

import "github.com/redis/go-redis/v9"

type RedisClient struct {
	Client   *redis.Client
	addr     string
	password string
	db       int
	protocol int
}

func (rdc *RedisClient) createClient() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     rdc.addr,
		Password: rdc.password,
		DB:       rdc.db,
		Protocol: rdc.protocol,
	})

	return client
}

func newRedisSingleton() *RedisClient {
	singleton := RedisClient{
		addr:     "redis-rinha:6379",
		password: "",
		db:       0,
		protocol: 2,
	}
	singleton.Client = singleton.createClient()
	return &singleton
}

var RedisClientSingleton = newRedisSingleton()
