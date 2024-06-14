package pub_sub

import (
	"context"
	"fmt"
	"log/slog"

	"cloud.google.com/go/pubsub"
)

type Consumer struct {
	logger *slog.Logger
	client *pubsub.Client
	ctx    context.Context

	projectId      string
	topicId        string
	subscriptionId string

	maxOutstandingMessages int

	DataStream chan *pubsub.Message
}

func New(ctx context.Context, logger *slog.Logger,
	projectId string, topicId string, subscriptionID string,
	maxOutstandingMessages int,
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

		maxOutstandingMessages: maxOutstandingMessages,

		DataStream: make(chan *pubsub.Message),
	}, nil
}

func (c *Consumer) Run() {
	sub := c.client.Subscription(c.subscriptionId)
	sub.ReceiveSettings = pubsub.ReceiveSettings{
		MaxOutstandingMessages: c.maxOutstandingMessages,
	}
	c.logger.Debug("subscribed to the subscription", slog.String("sub", c.subscriptionId))
	sub.Receive(c.ctx, func(ctx context.Context, msg *pubsub.Message) {
		c.DataStream <- msg
	})
	close(c.DataStream)
}
