package pub_sub

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"cloud.google.com/go/pubsub"
)

type Message struct {
	Id   int64
	Data []byte

	Error error
}

type Producer struct {
	logger *slog.Logger
	client *pubsub.Client
	ctx    context.Context

	projectId string
	topicId   string

	DataStream   chan Message
	StatusStream chan Message
}

func New(ctx context.Context, logger *slog.Logger,
	projectId string, topicId string,
) (*Producer, error) {
	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		logger.Error("error creating a pubsub client: %v", err)
		return nil, fmt.Errorf("error creating a pubsub client: %w", err)
	}

	return &Producer{
		ctx:    ctx,
		logger: logger,
		client: client,

		projectId: projectId,
		topicId:   topicId,

		DataStream:   make(chan Message),
		StatusStream: make(chan Message),
	}, nil
}

func (p *Producer) Run() {
	t := p.client.Topic(p.topicId)
	wg := sync.WaitGroup{}

MainStream:
	for msg := range p.DataStream {
		select {
		case <-p.ctx.Done():
			msg.Error = fmt.Errorf("context canceled")
			break MainStream
		default:
			wg.Add(1)
			go func(msg Message, ctx context.Context) {
				result := t.Publish(ctx, &pubsub.Message{
					Data: msg.Data,
				})

				_, err := result.Get(ctx)
				if err != nil {
					msg.Error = fmt.Errorf("error sending a message %w", err)
				}

				p.StatusStream <- msg
				wg.Done()
			}(msg, p.ctx)
		}
	}

	wg.Wait()
	close(p.StatusStream)
}
