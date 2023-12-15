package redis

import (
	"context"
	"github.com/gomodule/redigo/redis"
	"time"
)

// Client Redis 客户端.
type Client struct {
	options *ClientOptions
	pool    *redis.Pool
}

func NewClient(network, address, password string, opts ...ClientOption) *Client {
	c := Client{
		options: &ClientOptions{
			network:  network,
			address:  address,
			password: password,
		},
	}

	for _, opt := range opts {
		opt(c.options)
	}

	repairClient(c.options)

	pool := c.getRedisPool()
	return &Client{
		pool: pool,
	}
}

func (c *Client) getRedisPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     c.options.maxIdle,
		IdleTimeout: time.Duration(c.options.idleTimeoutSeconds) * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := c.getRedisConn()
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		MaxActive: c.options.maxActive,
		Wait:      c.options.wait,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func (c *Client) GetConn(ctx context.Context) (redis.Conn, error) {
	return c.pool.GetContext(ctx)
}

func (c *Client) getRedisConn() (redis.Conn, error) {
	if c.options.address == "" {
		panic("Cannot get redis address from config")
	}

	var dialOpts []redis.DialOption
	if len(c.options.password) > 0 {
		dialOpts = append(dialOpts, redis.DialPassword(c.options.password))
	}
	conn, err := redis.DialContext(context.Background(),
		c.options.network, c.options.address, dialOpts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *Client) SAdd(ctx context.Context, key, val string) (int, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()
	return redis.Int(conn.Do("SADD", key, val))
}

// Eval 支持使用 lua 脚本.
func (c *Client) Eval(ctx context.Context, src string, keyCount int, keysAndArgs []interface{}) (interface{}, error) {
	args := make([]interface{}, 2+len(keysAndArgs))
	args[0] = src
	args[1] = keyCount
	copy(args[2:], keysAndArgs)

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	return conn.Do("EVAL", args...)
}
