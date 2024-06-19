package rest

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type Consumer struct {
	markets []string
	start   time.Time
	end     time.Time

	waitQ   []*Task
	mutex   sync.Mutex
	running atomic.Uint32

	secondsToFetch uint64
	secondsFetched atomic.Uint64

	rps       uint8
	taskQuant time.Duration

	ctx    context.Context
	logger *slog.Logger

	ResponseChan chan *ResponseMessage
}

type ResponseMessage struct {
	Task  Task
	Start time.Time
	End   time.Time

	Data any
	Last bool
}

type Task struct {
	Market string
	Start  time.Time
	End    time.Time
}

func New(ctx context.Context, logger *slog.Logger,
	markets []string,
	start, end time.Time,
	rps uint8,
	taskQuant time.Duration,
) *Consumer {
	return &Consumer{
		markets: markets,
		start:   start,
		end:     end,

		rps:       rps,
		taskQuant: taskQuant,

		ctx:    ctx,
		logger: logger,

		secondsToFetch: uint64(end.Sub(start).Seconds() * float64(len(markets))),

		ResponseChan: make(chan *ResponseMessage),
	}
}

func (c *Consumer) Run() {
	for _, market := range c.markets {
		c.waitQ = append(c.waitQ, &Task{
			Market: market,
			Start:  c.start,
			End:    c.end,
		})
	}

	ticker := time.NewTicker(time.Second / time.Duration(c.rps))
	defer ticker.Stop()

	for {
		<-ticker.C

		c.mutex.Lock()

		if len(c.waitQ) > 0 {
			task := c.waitQ[0]
			c.waitQ = c.waitQ[1:]
			c.running.Add(1)
			go c.Execute(task)
		} else if c.running.Load() == 0 {
			close(c.ResponseChan)
			break
		}

		c.mutex.Unlock()
	}
}

func (c *Consumer) Execute(task *Task) {
	message, err := c.ApiCall(*task)
	if err != nil {
		c.logger.Error("error while fetching data from external API",
			slog.String("error", err.Error()),
		)
		c.mutex.Lock()
		defer c.mutex.Unlock()

		c.waitQ = append(c.waitQ, task)
	} else {
		secondsFetched := uint64(message.End.Sub(message.Start).Seconds())

		c.ResponseChan <- message

		if task.Start.Before(message.Start) && !message.Last {
			c.secondsFetched.Add(secondsFetched)

			c.mutex.Lock()
			defer c.mutex.Unlock()

			task.End = message.Start.Add(-time.Microsecond)
			if task.Start.Add(c.taskQuant * 2).Before(task.End) {
				newTask := splitTaskInplace(task)
				c.waitQ = append(c.waitQ, &newTask)
			}
			c.waitQ = append(c.waitQ, task)
		} else {
			c.secondsFetched.Add(uint64(task.End.Sub(task.Start).Seconds()))
		}
	}
	c.running.Add(^uint32(0))
}

func (c *Consumer) Status() uint8 {
	return uint8(float64(c.secondsFetched.Load()) / float64(c.secondsToFetch) * float64(100))
}

func splitTaskInplace(task *Task) Task {
	gap := task.End.Sub(task.Start)

	start := task.Start
	end := start.Add(gap / 2)

	task.Start = end.Add(time.Microsecond)

	newTask := Task{
		Market: task.Market,
		Start:  start,
		End:    end,
	}

	return newTask
}

func (c *Consumer) RedoTask(task *Task) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.secondsFetched.Add(^uint64(task.End.Sub(task.Start).Seconds() + 1))
	c.waitQ = append(c.waitQ, task)
}

func (c *Consumer) ApiCall(task Task) (*ResponseMessage, error) {
	panic(errors.New("not implemented"))
}
