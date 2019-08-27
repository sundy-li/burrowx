package monitor

import (
	"context"

	"github.com/sundy-li/burrowx/config"
)

type Fetcher struct {
	cfg     *config.Config
	clients []*KafkaClient
	cancels []context.CancelFunc
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewFetcher(cfg *config.Config) (f *Fetcher, err error) {
	ctx, cancel := context.WithCancel(context.Background())

	f = &Fetcher{
		clients: make([]*KafkaClient, 0, len(cfg.Kafka)),
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
	}
	for k, _ := range cfg.Kafka {
		c, cancel := context.WithCancel(f.ctx)
		f.cancels = append(f.cancels, cancel)
		client, e := NewKafkaClient(cfg, k, c)
		if e != nil {
			err = e
			return
		}
		f.clients = append(f.clients, client)
	}
	return
}

func (f *Fetcher) Start() {
	for _, cli := range f.clients {
		cli.Start()
	}
}

func (f *Fetcher) Stop() {
	f.cancel()
	for _, cli := range f.clients {
		cli.Stop()
	}
}
