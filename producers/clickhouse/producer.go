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

func NewProducer(ctx context.Context, logger *slog.Logger,
	host, database, username, password string, writeInterval time.Duration) (*Producer, error) {

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

	return &Producer{
		ctx:    ctx,
		logger: logger,

		writeInterval: writeInterval,

		conn:     &conn,
		host:     host,
		database: database,
		username: username,
		password: password,

		DataStream:   make(chan Message),
		StatusStream: make(chan []Message),
	}, nil
}

func (p *Producer) Run() {
	defer close(p.StatusStream)
	conn := *p.conn
	batch, err := conn.PrepareBatch(p.ctx, "INSERT INTO "+p.table)
	if err != nil {
		p.logger.Error("failed to prepare batch", slog.String("err", err.Error()))
	}

	p.lastSend = time.Now()
	localBatch := make([]Message, 0)
	for msg := range p.DataStream {
		err := batch.Append(msg.Data...)
		if err != nil {
			msg.Err = err
			p.logger.Error("failed to append data to batch", slog.String("err", err.Error()))
			p.StatusStream <- []Message{msg}
		} else {
			localBatch = append(localBatch, msg)
		}

		if time.Since(p.lastSend) > p.writeInterval {
			err := batch.Send()
			if err != nil {
				for _, localMsg := range localBatch {
					localMsg.Err = err
				}

				p.logger.Error("failed to send batch", slog.String("err", err.Error()))
			}
			p.StatusStream <- localBatch

			batch, err = conn.PrepareBatch(p.ctx, "INSERT INTO "+p.table)
			if err != nil {
				p.logger.Error("failed to prepare batch", slog.String("err", err.Error()))
			}
			localBatch = make([]Message, 0)
			p.lastSend = time.Now()
		}
	}

	if len(localBatch) > 0 {
		batch.Send()
		if err != nil {
			for _, localMsg := range localBatch {
				localMsg.Err = err
			}

			p.logger.Error("failed to send batch", slog.String("err", err.Error()))
		}
		p.StatusStream <- localBatch
	}
}
