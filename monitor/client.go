package monitor

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/cihub/seelog"
	"github.com/sundy-li/burrowx/config"
	"github.com/sundy-li/burrowx/model"
	"github.com/sundy-li/burrowx/outputs"
	"github.com/sundy-li/burrowx/outputs/kafka"
)

type KafkaClient struct {
	cluster        string
	cfg            *config.Config
	client         sarama.Client
	topicMap       map[string]int
	topic2Consumer map[string][]string

	ctx             context.Context
	stopped         chan int
	schemaUpdateMtx *sync.RWMutex

	brokerOffsetTicker *time.Ticker

	topicOffsetMapLock *sync.RWMutex
	//topic => parition => offset
	topicOffset map[string]map[int32]int64

	ops []outputs.Output

	topicFilterRegexps []*regexp.Regexp
	groupFilterRegexps []*regexp.Regexp
}

type BrokerTopicRequest struct {
	Result chan int
	Topic  string
}

var (
	// we may use a fixed interval
	METRIC_FETCH_INTERVAL_SECOND = 10
	META_UPDATE_INTERVAL_SECOND  = 60
)

func NewKafkaClient(cfg *config.Config, cluster string, ctx context.Context) (*KafkaClient, error) {
	// Set up sarama config from profile

	var kfk = cfg.Kafka[cluster]

	clientConfig, err := kafka.BuildKafkaConfig(kfk.ClientProfile, kfk.Sasl, kfk.Version)
	if err != nil {
		return nil, err
	}

	sclient, err := sarama.NewClient(strings.Split(kfk.Brokers, ","), clientConfig)
	if err != nil {
		return nil, err
	}

	client := &KafkaClient{
		cluster:        cluster,
		cfg:            cfg,
		client:         sclient,
		topicMap:       make(map[string]int),
		topic2Consumer: make(map[string][]string),

		schemaUpdateMtx: &sync.RWMutex{},
		stopped:         make(chan int),
		ctx:             ctx,

		topicOffset:        make(map[string]map[int32]int64),
		topicOffsetMapLock: &sync.RWMutex{},
	}

	for _, m := range cfg.Outputs {
		// start to build outputs
		bs, _ := json.Marshal(m)
		typ := m["type"].(string)
		op, err := outputs.NewOutput(typ, ctx, bs)
		if err != nil {
			return nil, err
		}
		client.ops = append(client.ops, op)
	}

	// TopicFilter
	{
		if cfg.General.TopicFilter == "" {
			cfg.General.TopicFilter = ".*"
		}
		patterns := strings.Split(cfg.General.TopicFilter, ",")
		client.topicFilterRegexps = make([]*regexp.Regexp, 0, 10)
		for _, p := range patterns {
			client.topicFilterRegexps = append(client.topicFilterRegexps, regexp.MustCompile(p))
		}
	}

	// GroupFilter
	{
		if cfg.General.GroupFilter == "" {
			cfg.General.GroupFilter = ".*"
		}
		patterns := strings.Split(cfg.General.GroupFilter, ",")
		client.groupFilterRegexps = make([]*regexp.Regexp, 0, 10)
		for _, p := range patterns {
			client.groupFilterRegexps = append(client.groupFilterRegexps, regexp.MustCompile(p))
		}
	}

	return client, nil
}

func (client *KafkaClient) Start() {
	for _, opt := range client.ops {
		opt.Start()
	}
	go func() {
		// Start the main processor goroutines for __consumer_offsets messages
		client.RefreshMetaData()
		client.getOffsets()

		client.brokerOffsetTicker = time.NewTicker(time.Duration(METRIC_FETCH_INTERVAL_SECOND) * time.Second)
		go func() {
		FOR:
			for {
				select {
				case <-client.brokerOffsetTicker.C:
					client.getOffsets()
				case <-client.ctx.Done():
					close(client.stopped)
					// Stop the offset checker and the topic metdata refresh and request channel
					client.brokerOffsetTicker.Stop()
					break FOR
				}
			}
		}()

		// Refresh metadata
		go func() {
			ticker := time.NewTicker(time.Duration(META_UPDATE_INTERVAL_SECOND) * time.Second)
			for _ = range ticker.C {
				client.RefreshMetaData()
			}
		}()
	}()
}

func (client *KafkaClient) Stop() {
	<-client.stopped
	for _, opt := range client.ops {
		opt.Stop()
	}
}

// This function performs massively parallel OffsetRequests, which is better than Sarama's internal implementation,
// which does one at a time. Several orders of magnitude faster.
func (client *KafkaClient) getOffsets() error {
	var (
		offsetsReqs = make(map[int32]*sarama.OffsetRequest)
		brokers     = make(map[int32]*sarama.Broker)
		offsetReqWg sync.WaitGroup
	)

	client.schemaUpdateMtx.Lock()
	defer client.schemaUpdateMtx.Unlock()

	// Generate an OffsetRequest for each topic:partition and bucket it to the leader broker
	for topic, partitions := range client.topicMap {
		for i := 0; i < partitions; i++ {
			broker, err := client.client.Leader(topic, int32(i))
			if err != nil {
				log.Errorf("Topic leader error on %s:%v: %v", topic, int32(i), err)
				return err
			}
			if _, ok := offsetsReqs[broker.ID()]; !ok {
				offsetsReqs[broker.ID()] = &sarama.OffsetRequest{}
			}
			brokers[broker.ID()] = broker
			offsetsReqs[broker.ID()].AddBlock(topic, int32(i), sarama.OffsetNewest, 1)
		}
	}

	offsetReqFunc := func(brokerId int32, request *sarama.OffsetRequest) {
		defer offsetReqWg.Done()
		response, err := brokers[brokerId].GetAvailableOffsets(request)
		if err != nil {
			log.Errorf("Cannot fetch offsets from broker %v: %v", brokerId, err)
			_ = brokers[brokerId].Close()
			return
		}
		topicOffsetMap := make(map[string]map[int32]int64)
		for topic, partitions := range response.Blocks {
			if _, ok := topicOffsetMap[topic]; !ok {
				topicOffsetMap[topic] = map[int32]int64{}
			}
			tp := topicOffsetMap[topic]
			for partition, offsetResponse := range partitions {
				if offsetResponse.Err != sarama.ErrNoError {
					log.Warnf("Error in OffsetResponse for %s:%v from broker %v: %s", topic, partition, brokerId, offsetResponse.Err.Error())
					return
				}
				tp[partition] = offsetResponse.Offsets[0]
			}
		}
		client.MergeMaps(topicOffsetMap)
	}
	//initial
	client.topicOffset = make(map[string]map[int32]int64)
	for brokerId, request := range offsetsReqs {
		offsetReqWg.Add(1)
		go offsetReqFunc(brokerId, request)

	}
	offsetReqWg.Wait()
	client.offsetFetchImport()
	return nil
}

func (client *KafkaClient) offsetFetchImport() {
	var ts = time.Now().Unix() / int64(METRIC_FETCH_INTERVAL_SECOND) * int64(METRIC_FETCH_INTERVAL_SECOND) * 1000
	//offset manager
	for topic, consumers := range client.topic2Consumer {
		for _, consumer := range consumers {
			msg := &model.ConsumerFullOffset{
				Cluster:      client.cluster,
				Topic:        topic,
				Group:        consumer,
				Timestamp:    ts,
				PartitionMap: make(map[int32]model.LogOffset),
			}

			manager, _ := sarama.NewOffsetManagerFromClient(consumer, client.client)
			defer manager.Close()
			var parition int32
			for parition = 0; parition < int32(client.topicMap[topic]); parition++ {
				pmanager, _ := manager.ManagePartition(topic, parition)
				offset, _ := pmanager.NextOffset()

				logOffset := model.LogOffset{
					Logsize: client.topicOffset[topic][parition],
					Offset:  offset,
				}
				if logOffset.Logsize < logOffset.Offset && logOffset.Logsize != 0 {
					logOffset.Offset = logOffset.Logsize
				}
				msg.PartitionMap[parition] = logOffset
			}
			if len(msg.PartitionMap) > 0 {
				for _, opt := range client.ops {
					opt.SaveMessage(msg)
				}
			}
		}
	}
}

// MergeMaps merge the offset of the topic
func (client *KafkaClient) MergeMaps(topicOffsetMap map[string]map[int32]int64) {
	withWriteLock(client.topicOffsetMapLock, func() {
		for topic, topicOffset := range topicOffsetMap {
			if _, ok := client.topicOffset[topic]; !ok {
				client.topicOffset[topic] = topicOffset
			} else {
				for partition, offset := range topicOffset {
					client.topicOffset[topic][partition] = offset
				}
			}
		}
	})
}

func (client *KafkaClient) RefreshMetaData() {
	client.schemaUpdateMtx.Lock()
	defer client.schemaUpdateMtx.Unlock()

	topics, _ := client.client.Topics()
	//filter topic by topicFilter
	for _, topic := range topics {
		if topic == "__consumer_offsets" {
			continue
		}
		for _, reg := range client.topicFilterRegexps {
			if reg.MatchString(topic) {
				partitions, _ := client.client.Partitions(topic)
				client.topicMap[topic] = len(partitions)
				break
			}
		}

	}

	// list groups
	groups := map[string]bool{}
	for _, broker := range client.client.Brokers() {
		if ok, _ := broker.Connected(); !ok {
			broker.Open(client.client.Config())
		}
		resp, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			log.Warnf("ListGroups error : %v", err)
			continue
		}
		for group := range resp.Groups {
			groups[group] = true
		}
	}
	groupList := make([]string, 0, len(groups))
	for group := range groups {
		if group == "" {
			continue
		}
		for _, reg := range client.groupFilterRegexps {
			if reg.MatchString(group) {
				groupList = append(groupList, group)
				break
			}
		}
	}

	//group description
	topic2Consumer := map[string]map[string]bool{}
	groupsPerBroker := make(map[*sarama.Broker][]string)
	for _, group := range groupList {
		controller, err := client.client.Coordinator(group)
		if err != nil {
			log.Warnf("Coordinator group:%s error : %v", group, err)
			return
		}
		groupsPerBroker[controller] = append(groupsPerBroker[controller], group)
	}

	for broker, brokerGroups := range groupsPerBroker {
		response, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{
			Groups: brokerGroups,
		})
		if err != nil {
			log.Warnf("get groupDescribe fail:%v", err)
			continue
		}
		for _, desc := range response.Groups {
			for _, gmd := range desc.Members {
				metadata, err2 := gmd.GetMemberMetadata()
				if err2 != nil {
					log.Warnf("GetMemberMetadata error : %v", err)
					continue
				} else {
					for _, topic := range metadata.Topics {
						if _, ok := client.topicMap[topic]; !ok {
							continue
						}
						if _, ok := topic2Consumer[topic]; !ok {
							topic2Consumer[topic] = make(map[string]bool)
						}
						topic2Consumer[topic][desc.GroupId] = true
					}
				}
			}
		}
	}

	for topic, consumerMap := range topic2Consumer {
		client.topic2Consumer[topic] = make([]string, 0, len(consumerMap))
		for group := range consumerMap {
			if group == "" {
				continue
			}
			for _, reg := range client.groupFilterRegexps {
				if reg.MatchString(group) {
					break
				}
			}
			client.topic2Consumer[topic] = append(client.topic2Consumer[topic], group)
		}
	}
	log.Debugf("topic2Consumer %v \n", client.topic2Consumer)
}

func withWriteLock(lock *sync.RWMutex, fn func()) {
	lock.Lock()
	defer lock.Unlock()
	fn()
}

func withReadLock(lock *sync.RWMutex, fn func()) {
	lock.RLock()
	defer lock.RUnlock()
	fn()
}
