package pub_sub_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/veska-io/connector/producers/pub_sub"
	pbstms "github.com/veska-io/proto-streams/gen/go/streams"
	"google.golang.org/protobuf/proto"
)

func TestProducer(t *testing.T) {
	ctx := context.Background()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	producer, error := pub_sub.New(
		ctx,
		logger,
		"datafi-415911",
		"trades-stream-test",
	)

	if error != nil {
		t.Errorf("error creating a pubsub producer: %v", error)
	}

	go producer.Run()

	go func() {
		for i := 0; i < 10; i++ {
			var msg []byte

			id := fmt.Sprint(i)
			trade := &pbstms.Trade{
				Id:        &id,
				Side:      "BUY",
				Market:    "BTC_USD",
				Price:     10000.0,
				Size:      0.1,
				Timestamp: 1234567892,
			}

			msg, err := proto.Marshal(trade)
			if err != nil {
				t.Errorf("proto.Marshal err: %v", err)
			}

			producer.DataStream <- pub_sub.Message{
				Id:   int64(i),
				Data: msg,
			}
		}

		close(producer.DataStream)
	}()

	for msg := range producer.StatusStream {
		if msg.Error != nil {
			t.Errorf("error sending message: %v", msg.Error)
		}
	}
}
