package pub_sub

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
)

type Consumer struct {
	logger *slog.Logger
	client *pubsub.Client
	ctx    context.Context

	projectId      string
	topicId        string
	subscriptionId string

	DataStream chan *pubsub.Message
}

func New(ctx context.Context, logger *slog.Logger,
	projectId string, topicId string, subscriptionID string,
) (*Consumer, error) {
	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		logger.Error("error creating a pubsub client: %v", err)
		return nil, fmt.Errorf("error creating a pubsub client: %w", err)
	}

	return &Consumer{
		ctx:    ctx,
		logger: logger,
		client: client,

		projectId:      projectId,
		topicId:        topicId,
		subscriptionId: subscriptionID,

		DataStream: make(chan *pubsub.Message),
	}, nil
}

func (c *Consumer) Run() {
	ctx2, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()
	sub := c.client.Subscription(c.subscriptionId)
	var mu sync.Mutex
	sub.Receive(ctx2, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()

		c.DataStream <- msg
	})
	close(c.DataStream)
}
