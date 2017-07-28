package protocol

type PartitionOffsetLag struct {
	Cluster             string
	Topic               string
	Partition           int32
	MaxOffset           int64
	Offset              int64
	Group               string
	Timestamp           int64
	TopicPartitionCount int
}
