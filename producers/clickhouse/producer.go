package clickhouse

import (
	"context"
	"crypto/tls"

	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Message struct {
	Data []any
	Meta any
	Err  error
}

type Producer struct {
	ctx    context.Context
	logger *slog.Logger

	lastSend      time.Time
	writeInterval time.Duration

	conn     *driver.Conn
	host     string
	database string
	username string
	password string
	table    string

	DataStream   chan Message
	StatusStream chan []Message
}

func New(ctx context.Context, logger *slog.Logger,
	host, database, username, password, table string, writeInterval time.Duration,
) (*Producer, error) {

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{host + ":9440"},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		TLS: &tls.Config{},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open clickhouse connection: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			logger.Error("clickhouse ping failed",
				slog.Int64("code", int64(exception.Code)),
				slog.String("message", exception.Message),
				slog.String("trace", exception.StackTrace),
			)
		}
		return nil, err
	}

	return &Producer{
		ctx:    ctx,
		logger: logger,

		writeInterval: writeInterval,

		conn:     &conn,
		host:     host,
		database: database,
		username: username,
		password: password,
		table:    table,

		DataStream:   make(chan Message),
		StatusStream: make(chan []Message),
	}, nil
}

func (p *Producer) Run() {
	defer close(p.StatusStream)
	batch := p.mustPrepareBatch()
	localBatch := make([]Message, 0)
	ticker := time.NewTicker(p.writeInterval)

MainStream:
	for {
		select {
		case <-p.ctx.Done():
			p.logger.Info("terminating producer")
			break MainStream
		case msg, ok := <-p.DataStream:
			if !ok {
				break MainStream
			}
			err := batch.Append(msg.Data...)
			if err != nil {
				msg.Err = err
				p.logger.Error("failed to append data to batch", slog.String("err", err.Error()))
				p.StatusStream <- []Message{msg}
			} else {
				localBatch = append(localBatch, msg)
			}
		case <-ticker.C:
			if len(localBatch) > 0 {
				err := batch.Send()
				if err != nil {
					for _, localMsg := range localBatch {
						localMsg.Err = err
					}

					p.logger.Error("failed to send batch", slog.String("err", err.Error()))
				}
				p.StatusStream <- localBatch

				batch = p.mustPrepareBatch()
				localBatch = make([]Message, 0)
			}
		}
	}

	if len(localBatch) > 0 {
		err := batch.Send()
		if err != nil {
			for _, localMsg := range localBatch {
				localMsg.Err = err
			}

			p.logger.Error("failed to send batch", slog.String("err", err.Error()))
		}
		p.StatusStream <- localBatch
	}
}

func (p *Producer) mustPrepareBatch() driver.Batch {
	conn := *p.conn
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+p.table)
	if err != nil {
		panic(err)
	}

	return batch
}
