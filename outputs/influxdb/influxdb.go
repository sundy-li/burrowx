package monitor

import (
	"encoding/json"
	"fmt"
	"time"

	client "github.com/influxdata/influxdb/client/v2"

	log "github.com/cihub/seelog"
	"github.com/sundy-li/burrowx/model"
)

type InfluxdbOutput struct {
	msgs chan *model.ConsumerFullOffset

	cfg *InfluxdbConfig

	threshold  int
	maxTimeGap int64
	influxdb   client.Client
	stopped    chan struct{}
}

type InfluxdbConfig struct {
	Hosts    string
	Username string
	Password string
	Db       string
}

func NewImporter(configStr string) (i *InfluxdbOutput, err error) {
	var cfg InfluxdbConfig

	json.Unmarshal([]byte(configStr), &cfg)

	i = &InfluxdbOutput{
		msgs:       make(chan *model.ConsumerFullOffset, 1000),
		threshold:  10,
		maxTimeGap: 10,
		stopped:    make(chan struct{}),
	}
	// Create a new HTTPClient
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     cfg.Hosts,
		Username: cfg.Username,
		Password: cfg.Password,
	})
	if err != nil {
		return
	}
	i.cfg = &cfg
	i.influxdb = c
	return
}

func (i InfluxdbOutput) Start() error {
	go func() {
		bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  i.cfg.Db,
			Precision: "s",
		})
		lastCommit := time.Now().Unix()
		for msg := range i.msgs {
			tags := map[string]string{
				"topic":          msg.Topic,
				"consumer_group": msg.Group,
				"cluster":        msg.Cluster,
			}

			for partition, entry := range msg.PartitionMap {
				//offset is the sql keyword, so we use offsize
				tags["partition"] = fmt.Sprintf("%d", partition)

				fields := map[string]interface{}{
					"logsize": entry.Logsize,
					"offsize": entry.Offset,
					"lag":     entry.Logsize - entry.Offset,
				}
				if entry.Offset < 0 {
					fields["lag"] = -1
					continue
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
					Database:  i.cfg.Db,
					Precision: "s",
				})
				lastCommit = time.Now().Unix()
			}
		}
		i.stopped <- struct{}{}
	}()

	return nil
}

func (i InfluxdbOutput) SaveMsg(msg *model.ConsumerFullOffset) {
	i.msgs <- msg
}

func (i InfluxdbOutput) Stop() error {
	close(i.msgs)
	<-i.stopped
	return nil
}

// runCmd method is for influxb querys
func (i InfluxdbOutput) runCmd(cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: i.cfg.Db,
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
