package monitor

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/cihub/seelog"
	"github.com/sundy-li/burrowx/config"
)

type KafkaClient struct {
	cluster            string
	cfg                *config.Config
	client             sarama.Client
	masterConsumer     sarama.Consumer
	partitionConsumers []sarama.PartitionConsumer
	messageChannel     chan *sarama.ConsumerMessage
	errorChannel       chan *sarama.ConsumerError
	wgFanIn            sync.WaitGroup
	wgProcessor        sync.WaitGroup
	topicMap           map[string]int
	topicMapLock       sync.RWMutex
	brokerOffsetTicker *time.Ticker

	consumerOffsetMapLock sync.RWMutex
	consumerOffset        map[string]map[int32]map[string]*ConsumerOffset //topic => partition => group => offset

	topicOffsetMapLock sync.RWMutex
	topicOffset        map[string]*TopicFullOffset //topic => offset

	importer *Importer
}

type BrokerTopicRequest struct {
	Result chan int
	Topic  string
}

func NewKafkaClient(cfg *config.Config, cluster string) (*KafkaClient, error) {
	// Set up sarama config from profile
	clientConfig := sarama.NewConfig()
	profile := cfg.ClientProfile[cfg.Kafka[cluster].ClientProfile]
	clientConfig.ClientID = profile.ClientId
	clientConfig.Net.TLS.Enable = profile.TLS
	if profile.TLSCertFilePath == "" || profile.TLSKeyFilePath == "" || profile.TLSCAFilePath == "" {
		clientConfig.Net.TLS.Config = &tls.Config{}
	} else {
		caCert, err := ioutil.ReadFile(profile.TLSCAFilePath)
		if err != nil {
			return nil, err
		}
		cert, err := tls.LoadX509KeyPair(profile.TLSCertFilePath, profile.TLSKeyFilePath)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		clientConfig.Net.TLS.Config = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}
		clientConfig.Net.TLS.Config.BuildNameToCertificate()
	}
	clientConfig.Net.TLS.Config.InsecureSkipVerify = profile.TLSNoVerify

	sclient, err := sarama.NewClient(strings.Split(cfg.Kafka[cluster].Brokers, ","), clientConfig)
	if err != nil {
		return nil, err
	}

	// Create sarama master consumer
	master, err := sarama.NewConsumerFromClient(sclient)
	if err != nil {
		sclient.Close()
		return nil, err
	}

	importer, err := NewImporter(cfg)
	if err != nil {
		return nil, err
	}

	client := &KafkaClient{
		cluster:        cluster,
		cfg:            cfg,
		client:         sclient,
		masterConsumer: master,
		messageChannel: make(chan *sarama.ConsumerMessage, 1000),
		errorChannel:   make(chan *sarama.ConsumerError, 1000),
		wgFanIn:        sync.WaitGroup{},
		wgProcessor:    sync.WaitGroup{},
		topicMap:       make(map[string]int),
		topicMapLock:   sync.RWMutex{},

		//three map,holy shit
		consumerOffset:        make(map[string]map[int32]map[string]*ConsumerOffset),
		consumerOffsetMapLock: sync.RWMutex{},

		topicOffset:        make(map[string]*TopicFullOffset),
		topicOffsetMapLock: sync.RWMutex{},

		importer: importer,
	}

	return client, nil
}

func (client *KafkaClient) Start() {
	client.importer.start()
	// Start the main processor goroutines for __consumer_offsets messages
	client.wgProcessor.Add(2)
	go func() {
		defer client.wgProcessor.Done()
		for msg := range client.messageChannel {
			client.RefreshConsumerOffset(msg)
		}
	}()
	go func() {
		defer client.wgProcessor.Done()
		for err := range client.errorChannel {
			log.Errorf("Consume error on %s:%v: %v", err.Topic, err.Partition, err.Err)
		}
	}()

	client.RefreshTopicMap()
	client.getOffsets()
	//TODO
	client.brokerOffsetTicker = time.NewTicker(time.Duration(10) * time.Second)
	go func() {
		for _ = range client.brokerOffsetTicker.C {
			client.getOffsets()
		}
	}()

	// Get a partition count for the consumption topic
	log.Info("start to consumer from", client.cfg.Kafka[client.cluster].OffsetsTopic)
	partitions, err := client.client.Partitions(client.cfg.Kafka[client.cluster].OffsetsTopic)
	if err != nil {
		panic(err)
	}

	// Start consumers for each partition with fan in
	client.partitionConsumers = make([]sarama.PartitionConsumer, len(partitions))
	log.Infof("Starting consumers for %v partitions of %s in cluster %s", len(partitions), client.cfg.Kafka[client.cluster].OffsetsTopic, client.cluster)
	for i, partition := range partitions {
		pconsumer, err := client.masterConsumer.ConsumePartition(client.cfg.Kafka[client.cluster].OffsetsTopic, partition, sarama.OffsetNewest)
		if err != nil {
			panic(err)
		}
		client.partitionConsumers[i] = pconsumer
		client.wgFanIn.Add(2)
		go func() {
			defer client.wgFanIn.Done()
			for msg := range pconsumer.Messages() {
				client.messageChannel <- msg
			}
		}()
		go func() {
			defer client.wgFanIn.Done()
			for err := range pconsumer.Errors() {
				client.errorChannel <- err
			}
		}()
	}
}

func (client *KafkaClient) Stop() {
	// We don't really need to do a safe stop, because we're not maintaining offsets. But we'll do it anyways
	for _, pconsumer := range client.partitionConsumers {
		pconsumer.AsyncClose()
	}

	// Wait for the Messages and Errors channel to be fully drained.
	client.wgFanIn.Wait()
	close(client.errorChannel)
	close(client.messageChannel)
	client.wgProcessor.Wait()

	// Stop the offset checker and the topic metdata refresh and request channel
	client.brokerOffsetTicker.Stop()
	client.importer.stop()
}

// This function performs massively parallel OffsetRequests, which is better than Sarama's internal implementation,
// which does one at a time. Several orders of magnitude faster.
func (client *KafkaClient) getOffsets() error {
	// Start with refreshing the topic list
	client.RefreshTopicMap()

	requests := make(map[int32]*sarama.OffsetRequest)
	brokers := make(map[int32]*sarama.Broker)

	// Generate an OffsetRequest for each topic:partition and bucket it to the leader broker
	for topic, partitions := range client.topicMap {
		for i := 0; i < partitions; i++ {
			broker, err := client.client.Leader(topic, int32(i))
			if err != nil {
				log.Errorf("Topic leader error on %s:%v: %v", topic, int32(i), err)
				return err
			}
			if _, ok := requests[broker.ID()]; !ok {
				requests[broker.ID()] = &sarama.OffsetRequest{}
				log.Debug("Setbroker=>", broker.ID())
			}
			brokers[broker.ID()] = broker
			requests[broker.ID()].AddBlock(topic, int32(i), sarama.OffsetNewest, 1)
		}
	}

	// Send out the OffsetRequest to each broker for all the partitions it is leader for
	// The results go to the offset storage module
	var wg sync.WaitGroup

	getBrokerOffsets := func(brokerId int32, request *sarama.OffsetRequest) {
		defer wg.Done()
		response, err := brokers[brokerId].GetAvailableOffsets(request)
		if err != nil {
			log.Errorf("Cannot fetch offsets from broker %v: %v", brokerId, err)
			_ = brokers[brokerId].Close()
			return
		}
		ts := time.Now().Unix() * 1000

		topicOffsetMap := make(map[string]*TopicFullOffset)
		for topic, partitions := range response.Blocks {
			if _, ok := topicOffsetMap[topic]; !ok {
				topicOffsetMap[topic] = &TopicFullOffset{
					Cluster:      client.cluster,
					Topic:        topic,
					partitionMap: make(map[int32]int64),
					Timestamp:    ts,
				}
			}
			tp := topicOffsetMap[topic]

			for partition, offsetResponse := range partitions {
				if offsetResponse.Err != sarama.ErrNoError {
					log.Warnf("Error in OffsetResponse for %s:%v from broker %v: %s", topic, partition, brokerId, offsetResponse.Err.Error())
					return
				}
				if _, ok := tp.partitionMap[partition]; !ok {
					tp.Offset += offsetResponse.Offsets[0]
					tp.partitionMap[partition] = offsetResponse.Offsets[0]
				}
			}
		}
		client.MergeMaps(topicOffsetMap)
	}

	//initial
	client.topicOffset = make(map[string]*TopicFullOffset)
	for brokerId, request := range requests {
		wg.Add(1)
		go getBrokerOffsets(brokerId, request)
	}
	wg.Wait()
	client.CombineTopicAndConsumer()
	return nil
}

func (client *KafkaClient) MergeMaps(topicOffsetMap map[string]*TopicFullOffset) {
	client.topicOffsetMapLock.Lock()
	defer client.topicOffsetMapLock.Unlock()

	for topic, topicOffset := range topicOffsetMap {
		if _, ok := client.topicOffset[topic]; !ok {
			client.topicOffset[topic] = topicOffset
		} else {
			for partition, offset := range topicOffset.partitionMap {
				if _, ok2 := client.topicOffset[topic].partitionMap[partition]; !ok2 {
					client.topicOffset[topic].partitionMap[partition] = offset
					client.topicOffset[topic].Offset += offset

					log.Debugf("Topic offset setting %s:%d:%d", topic, partition, len(client.topicOffset[topic].partitionMap))
				}
			}
		}
	}
}

func (client *KafkaClient) CombineTopicAndConsumer() {
	for topic, offset := range client.topicOffset {
		count := client.GetPartitionCount(topic)
		if count != len(offset.partitionMap) {
			log.Warnf("Drop topic %s offset,except %d partition actually got %d", topic, count, len(offset.partitionMap))
			continue
		}
		result := make(map[string]*ConsumerFullOffset)
		if _, ok := client.consumerOffset[topic]; ok {
			for partition, partitionOffset := range offset.partitionMap {
				if _, ok := client.consumerOffset[topic][partition]; ok {
					for consumer, consumerOffset := range client.consumerOffset[topic][partition] {
						if _, ok := result[consumer]; !ok {
							result[consumer] = &ConsumerFullOffset{
								Cluster:      client.cluster,
								Topic:        topic,
								Group:        consumer,
								partitionMap: make(map[int32]int64),
								Timestamp:    offset.Timestamp,
							}
						}
						//judge time diff
						if offset.Timestamp-consumerOffset.Timestamp <= 60*1000 {
							if _, ok := result[consumer].partitionMap[partition]; !ok {
								result[consumer].partitionMap[partition] = consumerOffset.Offset

								if consumerOffset.Offset < 0 {
									consumerOffset.Offset = partitionOffset
								}
								result[consumer].Offset += consumerOffset.Offset
								result[consumer].MaxOffset += partitionOffset
							}
						} else {
							//ingore
							log.Debugf("Drop %s:%v offset by partition metric by time diff not match", topic, consumer)
						}
					}
				} else {
					log.Debugf("Drop %s:%d offset by partition metric match ", topic, partition)
				}
			}

		} else {
			//ignore
			log.Debugf("Topic %s not found any consumer", topic)
		}
		for consumer, consumerFullOffset := range result {
			if len(consumerFullOffset.partitionMap) == count {
				client.importer.saveMsg(consumerFullOffset)
				log.Debugf("Saving %s:%v offset", topic, consumer)
			} else {
				log.Debugf("Dropping %s:%v offset,not full fill expect %d partition metrics actually got %d ", topic, consumer, count, len(consumerFullOffset.partitionMap))
			}
		}
	}

}

func (client *KafkaClient) RefreshTopicMap() {
	client.topicMapLock.Lock()
	topics, _ := client.client.Topics()
	for _, topic := range topics {
		partitions, _ := client.client.Partitions(topic)
		client.topicMap[topic] = len(partitions)
	}
	client.topicMapLock.Unlock()
}

func (client *KafkaClient) GetPartitionCount(topic string) int {
	return client.topicMap[topic]
}

func (client *KafkaClient) RefreshConsumerOffset(msg *sarama.ConsumerMessage) {
	var keyver, valver uint16
	var group, topic string
	var partition uint32
	var offset, timestamp uint64

	buf := bytes.NewBuffer(msg.Key)
	err := binary.Read(buf, binary.BigEndian, &keyver)
	switch keyver {
	case 0, 1:
		group, err = readString(buf)
		if err != nil {
			log.Warnf("Failed to decode %s:%v offset %v: group", msg.Topic, msg.Partition, msg.Offset)
			return
		}
		topic, err = readString(buf)
		if err != nil {
			log.Warnf("Failed to decode %s:%v offset %v: topic", msg.Topic, msg.Partition, msg.Offset)
			return
		}
		err = binary.Read(buf, binary.BigEndian, &partition)
		if err != nil {
			log.Warnf("Failed to decode %s:%v offset %v: partition", msg.Topic, msg.Partition, msg.Offset)
			return
		}
	case 2:
		log.Debugf("Discarding group metadata message with key version 2")
		return
	default:
		log.Warnf("Failed to decode %s:%v offset %v: keyver %v", msg.Topic, msg.Partition, msg.Offset, keyver)
		return
	}
	buf = bytes.NewBuffer(msg.Value)
	err = binary.Read(buf, binary.BigEndian, &valver)
	if (err != nil) || ((valver != 0) && (valver != 1)) {
		log.Warnf("Failed to decode %s:%v offset %v: valver %v", msg.Topic, msg.Partition, msg.Offset, valver)
		return
	}
	err = binary.Read(buf, binary.BigEndian, &offset)
	if err != nil {
		log.Warnf("Failed to decode %s:%v offset %v: offset", msg.Topic, msg.Partition, msg.Offset)
		return
	}
	_, err = readString(buf)
	if err != nil {
		log.Warnf("Failed to decode %s:%v offset %v: metadata", msg.Topic, msg.Partition, msg.Offset)
		return
	}
	err = binary.Read(buf, binary.BigEndian, &timestamp)
	if err != nil {
		log.Warnf("Failed to decode %s:%v offset %v: timestamp", msg.Topic, msg.Partition, msg.Offset)
		return
	}

	co := &ConsumerOffset{
		Cluster:   client.cluster,
		Topic:     topic,
		Group:     group,
		Partition: int32(partition),
		Offset:    int64(offset),
		Timestamp: int64(timestamp),
	}
	client.consumerOffsetMapLock.Lock()
	if _, ok := client.consumerOffset[topic]; !ok {
		client.consumerOffset[topic] = make(map[int32]map[string]*ConsumerOffset)
	}
	if _, ok := client.consumerOffset[topic][int32(partition)]; !ok {
		client.consumerOffset[topic][int32(partition)] = make(map[string]*ConsumerOffset)
	}

	client.consumerOffset[topic][int32(partition)][co.Group] = co
	log.Debugf("setting %s:%v:%d offset:%d", topic, co.Group, partition, co.Offset)
	client.consumerOffsetMapLock.Unlock()
	return
}
func readString(buf *bytes.Buffer) (string, error) {
	var strlen uint16
	err := binary.Read(buf, binary.BigEndian, &strlen)
	if err != nil {
		return "", err
	}
	strbytes := make([]byte, strlen)
	n, err := buf.Read(strbytes)
	if (err != nil) || (n != int(strlen)) {
		return "", errors.New("string underflow")
	}
	return string(strbytes), nil
}

func genKey(topic, consumer string, partition int) string {
	return fmt.Sprintf("%s_%s_%d", topic, consumer, partition)
}

func getConsumerFromKey(key string) string {
	return strings.Split(key, "_")[1]
}
