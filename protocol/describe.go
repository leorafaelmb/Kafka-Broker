package protocol

type DescribeTPRequest struct {
	Topics                 []string // compact array => compact string
	ResponsePartitionLimit int32
	Cursor
}

type Cursor struct {
	TopicName      string // compact string
	PartitionIndex int32
}

type DescribeTPResponse struct {
	ThrottleTime int32
}

type Topic struct {
	ErrorCode                 int16
	Contents                  string // compact string
	TopicID                   int64
	IsInternal                bool
	Partitions                []Partition
	TopicAuthorizedOperations int32
	NextCursor                Cursor
}

type Partition struct {
	ErrorCode              int16
	PartitionIndex         int32
	LeaderID               int32
	LeaderEpoch            int32
	ReplicaNodes           int32
	ISRNodes               int32
	EligibleLeaderReplicas int32
	LastKnownELR           int32
	OfflineReplicas        int32
}

func Deserialize(rbr ReaderByteReader) DescribeTPRequest {

	return DescribeTPRequest{}
}
