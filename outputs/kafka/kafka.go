package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/cihub/seelog"
	"github.com/sundy-li/burrowx/config"
	"github.com/sundy-li/burrowx/model"
)

var (
	v0_10_2_1 = sarama.V0_10_2_0
	v0_11_0_0 = sarama.V0_10_2_0
)

type KafkaOutput struct {
	msgs    chan *model.ConsumerFullOffset
	stopped chan struct{}

	Brokers string
	Topic   string
	Version string

	Sasl          config.Sasl
	ClientProfile config.Profile

	producer sarama.AsyncProducer
	wg       sync.WaitGroup
	ctx      context.Context
}

func New(ctx context.Context, config []byte) (output *KafkaOutput, err error) {
	output = &KafkaOutput{
		ctx:     ctx,
		msgs:    make(chan *model.ConsumerFullOffset, 1000),
		stopped: make(chan struct{}),
	}
	json.Unmarshal(config, output)
	if err != nil {
		return
	}
	cfg, err := BuildKafkaConfig(output.ClientProfile, output.Sasl, output.Version)
	if err != nil {
		return
	}

	output.producer, err = sarama.NewAsyncProducer(strings.Split(output.Brokers, ","), cfg)
	if err != nil {
		return
	}

	output.wg.Add(2)
	go output.successWorker(output.producer.Successes())
	go output.errorWorker(output.producer.Errors())
	return
}

func (output *KafkaOutput) Start() error {
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

					output.producer.Input() <- &sarama.ProducerMessage{
						Topic: output.Topic,
						Key:   sarama.ByteEncoder([]byte("kafka.offset|" + time.Now().String())),
						Value: sarama.ByteEncoder(bs),
					}
				}
			case <-output.ctx.Done():
				close(output.stopped)
				break FOR
			}
		}
	}()

	return nil
}

func (output *KafkaOutput) SaveMessage(msg *model.ConsumerFullOffset) {
	output.msgs <- msg
}

func (output *KafkaOutput) Stop() error {
	close(output.msgs)
	<-output.stopped
	output.producer.AsyncClose()
	output.wg.Wait()
	return nil
}

func (output *KafkaOutput) successWorker(ch <-chan *sarama.ProducerMessage) {
	defer output.wg.Done()
	for range ch {

	}
}

func (output *KafkaOutput) errorWorker(ch <-chan *sarama.ProducerError) {
	defer output.wg.Done()
	for errMsg := range ch {
		log.Error("send msg error" + errMsg.Error())
	}
}
