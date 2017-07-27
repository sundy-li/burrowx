package monitor

import (
	"strconv"
	"time"

	client "github.com/influxdata/influxdb/client/v2"

	log "github.com/cihub/seelog"
	"github.com/sundy-li/burrowx/config"
	"github.com/sundy-li/burrowx/protocol"
)

type Importer struct {
	msgs chan *protocol.PartitionLag
	cfg  *config.Config

	threshold int
	influxdb  client.Client
	stopped   chan struct{}
}

func NewImporter(cfg *config.Config) (i *Importer, err error) {
	i = &Importer{
		msgs:      make(chan *protocol.PartitionLag, 1000),
		cfg:       cfg,
		threshold: 10,
		stopped:   make(chan struct{}),
	}
	// Create a new HTTPClient
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     cfg.Influxdb.Hosts,
		Username: cfg.Influxdb.Username,
		Password: cfg.Influxdb.Pwd,
	})
	if err != nil {
		return
	}
	i.influxdb = c
	return
}

func (i *Importer) start() {
	go func() {
		bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  i.cfg.Influxdb.Db,
			Precision: "s",
		})
		for msg := range i.msgs {
			tags := map[string]string{
				"topic":          msg.Topic,
				"consumer_group": msg.Group,
				"partition":      strconv.Itoa(int(msg.Partition)),
				"cluster":        msg.Cluster,
			}
			//offset is the sql keyword, so we use offsize
			fields := map[string]interface{}{
				"offsize": msg.Offset,
				"logsize": msg.MaxOffset,
				"lag":     msg.Lag,
			}

			tm := time.Unix(msg.Timestamp/1000, 0)
			pt, err := client.NewPoint(msg.Group, tags, fields, tm)
			if err != nil {
				log.Error("error in add point ", err.Error())
				continue
			}
			bp.AddPoint(pt)
			if len(bp.Points()) > i.threshold {
				err := i.influxdb.Write(bp)
				if err != nil {
					log.Error("error in insert points ", err.Error())
					continue
				}
			}
		}
		i.stopped <- struct{}{}
	}()

}

func (i *Importer) saveMsg(msg *protocol.PartitionLag) {
	i.msgs <- msg
}

func (i *Importer) stop() {
	close(i.msgs)
	<-i.stopped
}
