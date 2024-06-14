package pub_sub_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/veska-io/connector/consumers/pub_sub"
	pbstms "github.com/veska-io/proto-streams/gen/go/streams"
	"google.golang.org/protobuf/proto"
)

func TestConsumer(t *testing.T) {
	ctx := context.Background()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	consumer, error := pub_sub.New(
		ctx,
		logger,
		"datafi-415911",
		"trades-stream",
		"clickhouse-connector",
		10000,
	)

	if error != nil {
		t.Errorf("error creating a pubsub consumer: %v", error)
	}

	go consumer.Run()

	for msg := range consumer.DataStream {
		trade := &pbstms.Trade{}
		if err := proto.Unmarshal(msg.Data, trade); err != nil {
			t.Errorf("proto.Unmarshal err: %v", err)
			msg.Nack()
			return
		}
		msg.Ack()
		t.Logf("trade: %v", trade)
	}
}

func TestAck(t *testing.T) {
	subscribeWithProtoSchema("datafi-415911", "clickhouse-connector")
}

func subscribeWithProtoSchema(projectID, subID string) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("pubsub.NewClient: %w", err)
	}

	state := &pbstms.Trade{}

	sub := client.Subscription(subID)
	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var mu sync.Mutex

	sub.Receive(ctx2, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()

		if err := proto.Unmarshal(msg.Data, state); err != nil {
			msg.Nack()
			return
		}

		msg.Ack()
	})
	return nil
}
