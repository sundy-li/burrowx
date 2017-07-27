package monitor

import (
	"github.com/sundy-li/burrowx/config"
)

type Fetcher struct {
	cfg     *config.Config
	clients []*KafkaClient
}

func NewFetcher(cfg *config.Config) (f *Fetcher, err error) {
	f = &Fetcher{
		clients: make([]*KafkaClient, 0, len(cfg.Kafka)),
		cfg:     cfg,
	}
	for k, _ := range cfg.Kafka {
		client, e := NewKafkaClient(cfg, k)
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
	for _, cli := range f.clients {
		cli.Stop()
	}
}
