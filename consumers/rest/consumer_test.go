package rest_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	consumer "github.com/veska-io/connector/consumers/rest"
)

func TestTaskGenerator_Generate(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// markets := []string{"TON-USD", "DYDX-USD", "BONK-USD", "AAVE-USD", "HBAR-USD"}
	markets := []string{"BTC-USD"}
	now := time.Now().UTC()

	end := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, time.UTC)
	start := end.Add(-1 * time.Duration(time.Hour))
	t.Logf("Start: %s, End: %s", start, end)

	c := consumer.New(
		context.Background(),
		logger,
		markets,
		start,
		end,
		7,
		time.Second*60*60,
	)

	go c.Run()

	for range c.ResponseChan {
		// r, _ := response.Data.(consumer.TradesResponse)
		// t.Logf("%s %s %s", response.Task.Market, r.Trades[0].CreatedAt, r.Trades[len(r.Trades)-1].CreatedAt)
		t.Logf("%d", c.Status())
	}
}
