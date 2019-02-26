package monitor

import (
	"fmt"
	"time"

	client "github.com/influxdata/influxdb/client/v2"

	log "github.com/cihub/seelog"
	"github.com/sundy-li/burrowx/config"
)

type Importer struct {
	msgs chan *ConsumerFullOffset
	cfg  *config.Config

	threshold  int
	maxTimeGap int64
	influxdb   client.Client
	stopped    chan struct{}
}

func NewImporter(cfg *config.Config) (i *Importer, err error) {
	i = &Importer{
		msgs:       make(chan *ConsumerFullOffset, 1000),
		cfg:        cfg,
		threshold:  10,
		maxTimeGap: 10,
		stopped:    make(chan struct{}),
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
	// _, err := i.runCmd("create database " + i.cfg.Influxdb.Db)
	// if err != nil {
	// 	panic(err)
	// }
	go func() {
		bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  i.cfg.Influxdb.Db,
			Precision: "s",
		})
		lastCommit := time.Now().Unix()
		for msg := range i.msgs {
			tags := map[string]string{
				"topic":          msg.Topic,
				"consumer_group": msg.Group,
				"cluster":        msg.Cluster,
			}

			for partition, entry := range msg.partitionMap {
				//offset is the sql keyword, so we use offsize
				tags["partition"] = fmt.Sprintf("%d", partition)

				fields := map[string]interface{}{
					"logsize": entry.Logsize,
					"offsize": entry.Offset,
					"lag":     entry.Logsize - entry.Offset,
				}
				if entry.Offset < 0 {
					fields["lag"] = -1
				}

				tm := time.Unix(msg.Timestamp/1000, 0)
				pt, err := client.NewPoint("consumer_metrics", tags, fields, tm)
				if err != nil {
					log.Error("error in add point ", err.Error())
					continue
				}
				bp.AddPoint(pt)
			}

			if len(bp.Points()) > i.threshold || time.Now().Unix()-lastCommit >= i.maxTimeGap {
				err := i.influxdb.Write(bp)
				if err != nil {
					log.Error("error in insert points ", err.Error())
					continue
				}
				bp, _ = client.NewBatchPoints(client.BatchPointsConfig{
					Database:  i.cfg.Influxdb.Db,
					Precision: "s",
				})
				lastCommit = time.Now().Unix()
			}
		}
		i.stopped <- struct{}{}
	}()

}

func (i *Importer) saveMsg(msg *ConsumerFullOffset) {
	i.msgs <- msg
}

func (i *Importer) stop() {
	close(i.msgs)
	<-i.stopped
}

// runCmd method is for influxb querys
func (i *Importer) runCmd(cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: i.cfg.Influxdb.Db,
	}
	if response, err := i.influxdb.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil

}
