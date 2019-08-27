package outputs

import (
	"context"
	"errors"

	"github.com/sundy-li/burrowx/model"
	"github.com/sundy-li/burrowx/outputs/console"
	"github.com/sundy-li/burrowx/outputs/influxdb"
	"github.com/sundy-li/burrowx/outputs/kafka"
)

type Output interface {
	SaveMessage(msg *model.ConsumerFullOffset)
	Start() error
	Stop() error
}

func NewOutput(typ string, ctx context.Context, bs []byte) (opt Output, err error) {
	switch typ {
	case "kafka":
		opt, err = kafka.New(ctx, bs)
	case "influxdb":
		opt, err = influxdb.New(ctx, bs)
	case "console":
		opt, err = console.New(ctx, bs)
	default:
		err = errors.New("No Match output type:" + typ)
	}
	return
}
