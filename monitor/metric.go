package monitor

type TopicPartitionOffset struct {
	Cluster             string
	Topic               string
	Partition           int32
	Offset              int64
	Timestamp           int64
	Group               string
	TopicPartitionCount int
}

type ConsumerOffset struct {
	Cluster   string
	Topic     string
	Group     string
	Partition int32
	Offset    int64
	Timestamp int64
}

type ConsumerFullOffset struct {
	Cluster string
	Topic   string
	Group   string

	MaxOffset int64
	Offset    int64
	Lag       int64
	Timestamp int64

	PartitionCount int
	PartionMap     map[int32]bool
}
