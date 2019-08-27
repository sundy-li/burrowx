package console

import (
	"context"
	"encoding/json"
	"time"

	"github.com/sundy-li/burrowx/model"
)

type ConsoleOutput struct {
	msgs    chan *model.ConsumerFullOffset
	stopped chan struct{}
	ctx     context.Context
}

func New(ctx context.Context, config []byte) (output *ConsoleOutput, err error) {
	output = &ConsoleOutput{
		ctx:     ctx,
		msgs:    make(chan *model.ConsumerFullOffset, 1000),
		stopped: make(chan struct{}),
	}
	return
}

func (output *ConsoleOutput) Start() error {
	go func() {
	FOR:
		for {
			select {
			case msg := <-output.msgs:
				for partition, entry := range msg.PartitionMap {
					//offset is the sql keyword, so we use offsize
					fields := map[string]interface{}{
						"topic":          msg.Topic,
						"consumer_group": msg.Group,
						"cluster":        msg.Cluster,

						"its":       time.Now().Unix(),
						"partition": partition,
						"logsize":   entry.Logsize,
						"offsize":   entry.Offset,
						"lag":       entry.Logsize - entry.Offset,
					}

					bs, _ := json.Marshal(fields)

					println("Msg => ", string(bs))
				}
			case <-output.ctx.Done():
				close(output.stopped)
				break FOR
			}
		}

	}()

	return nil
}

func (output *ConsoleOutput) SaveMessage(msg *model.ConsumerFullOffset) {
	output.msgs <- msg
}

func (output *ConsoleOutput) Stop() error {
	close(output.msgs)
	<-output.stopped
	return nil
}
