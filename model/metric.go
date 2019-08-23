package model

type LogOffset struct {
	Logsize int64
	Offset  int64
}

type ConsumerOffset struct {
	Cluster   string
	Topic     string
	Group     string
	Partition int32
	Offset    int64
	Timestamp int64
}

type TopicPartitionOffset struct {
	Parition int32
	Offset   int64
}

type ConsumerFullOffset struct {
	Cluster   string
	Topic     string
	Group     string
	Timestamp int64

	PartitionMap map[int32]LogOffset
}
