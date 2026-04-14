package rdb

import (
	"context"

	"github.com/hibiken/asynq/internal/base"
	"github.com/redis/go-redis/v9"
)

type RedisPubSub struct {
	pubsub *redis.PubSub
}

func NewRedisPubSub(pubsub *redis.PubSub) base.PubSub {
	return &RedisPubSub{pubsub: pubsub}
}

func (p *RedisPubSub) Subscribe(ctx context.Context, channels ...string) error {
	return p.pubsub.Subscribe(ctx, channels...)
}

func (p *RedisPubSub) Channel() <-chan string {
	ch := make(chan string)

	go func() {
		for msg := range p.pubsub.Channel() {
			ch <- msg.Payload
		}
		close(ch)
	}()

	return ch
}

func (p *RedisPubSub) Close() error {
	return p.pubsub.Close()
}
