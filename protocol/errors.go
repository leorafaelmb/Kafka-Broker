package protocol

type Error struct {
	ErrorCode   int16
	Retriable   bool
	Description string
}

var (
	ErrNone                    = Error{ErrorCode: 0, Retriable: false, Description: ""}
	ErrUnknownTopicOrPartition = Error{ErrorCode: 3, Retriable: true, Description: "This server does not host this topic-partition."}
	ErrUnsupportedVersion      = Error{ErrorCode: 35, Retriable: false, Description: "The version of API is not supported."}
)

var ErrorMap = map[int16]Error{
	0: ErrNone,
	3: ErrUnknownTopicOrPartition,
}
