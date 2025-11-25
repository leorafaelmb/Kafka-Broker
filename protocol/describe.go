package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type DescribeTPRequest struct {
	Topics                 CompactArray[CompactString]
	ResponsePartitionLimit uint32
	Cursor                 *Cursor
}

type Cursor struct {
	TopicName      CompactString
	PartitionIndex uint32
}

type DescribeTPResponse struct {
	ThrottleTime uint32
	Topics       CompactArray[*Topic]
	Cursor       *Cursor
}

type Topic struct {
	ErrorCode                 uint16
	Contents                  CompactString
	TopicID                   [16]byte
	IsInternal                bool
	Partitions                CompactArray[*Partition]
	TopicAuthorizedOperations uint32
}

type Partition struct {
	ErrorCode              uint16
	PartitionIndex         uint32
	LeaderID               uint32
	LeaderEpoch            uint32
	ReplicaNodes           uint32
	ISRNodes               uint32
	EligibleLeaderReplicas uint32
	LastKnownELR           uint32
	OfflineReplicas        uint32
}

const PartitionLen = 35

func deserializeDescribeRequest(body []byte) (DescribeTPRequest, error) {
	dtpr := DescribeTPRequest{}
	rbr := bytes.NewBuffer(body)
	c := &Cursor{}
	var err error

	dtpr.Topics, err = ReadCompactArray(rbr)
	if err != nil {
		return dtpr, err
	}

	if err = binary.Read(rbr, binary.BigEndian, &dtpr.ResponsePartitionLimit); err != nil {
		return dtpr, err
	}

	// they really need to put this in the official kwp docs...
	// https://cwiki.apache.org/confluence/display/KAFKA/KIP-893%3A+The+Kafka+protocol+should+support+nullable+structs
	// byte represents whether this struct is present or not
	var cursorLen byte
	cursorLen, err = rbr.ReadByte()
	if err != nil {
		fmt.Println(err.Error())
		return dtpr, err
	}
	if int8(cursorLen) != -1 {
		topicName, err := ReadCompactString(rbr)
		if err != nil {
			return dtpr, err
		}

		var partitionIndex uint32

		if err = binary.Read(rbr, binary.BigEndian, &partitionIndex); err != nil {
			return dtpr, err
		}

		c.TopicName = topicName
		c.PartitionIndex = partitionIndex

	} else {
		c = nil
	}
	dtpr.Cursor = c

	if err = DiscardTaggedFields(rbr); err != nil {
		return dtpr, err
	}

	return dtpr, nil
}

func getDescribeTopicPartitionsResponse(req RequestHeader) []byte {
	dtpRequest, err := deserializeDescribeRequest(req.Body)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}

	var throttleTime uint32 = 0
	topics := dtpRequest.processTopics()
	var cursor *Cursor = nil

	serTopics := topics.Serialize()
	serCursor := cursor.Serialize()
	fmt.Println(serTopics)

	msgSize := uint32(10 + len(serTopics) + len(serCursor))

	resp := make([]byte, msgSize+4)
	binary.BigEndian.PutUint32(resp[0:4], msgSize)
	binary.BigEndian.PutUint32(resp[4:8], uint32(req.CorrelationID))
	binary.BigEndian.PutUint32(resp[8:12], throttleTime)
	copy(resp[13:13+len(serTopics)], serTopics)
	copy(resp[13+len(serTopics):], serCursor)
	fmt.Println(resp)

	return resp

}

func (d *DescribeTPRequest) processTopics() CompactArray[*Topic] {
	topics := make(CompactArray[*Topic], len(d.Topics))

	for i, topicName := range d.Topics {
		topics[i] = newTopicResponse(topicName)
	}

	return topics

}

func (d DescribeTPResponse) Size() uint32 {
	var topicsLen uint32 = 0
	if d.Topics != nil {
		for _, t := range d.Topics {
			topicsLen += t.Size()
		}
	}

	var cursorLen uint32 = 0
	if d.Cursor != nil {
		cursorLen = d.Cursor.Size()
	}

	return 4 + topicsLen + cursorLen
}

func newTopicResponse(topicName CompactString) *Topic {
	var (
		// error code always 3 for now
		errorCode                                     = uint16(ErrUnknownTopicOrPartition.ErrorCode)
		contents                                      = topicName
		topicID                                       = [16]byte{}
		isInternal                                    = false
		partitions           CompactArray[*Partition] = nil
		authorizedOperations uint32                   = 0
	)

	return &Topic{
		ErrorCode:                 errorCode,
		Contents:                  contents,
		TopicID:                   topicID,
		IsInternal:                isInternal,
		Partitions:                partitions,
		TopicAuthorizedOperations: authorizedOperations,
	}
}

func (t *Topic) Serialize() []byte {
	fmt.Println("hey")
	resp := make([]byte, t.Size())
	serializedContents := t.Contents.Serialize()
	binary.BigEndian.PutUint16(resp[0:2], t.ErrorCode)
	copy(resp[2:2+len(serializedContents)], serializedContents)
	marker := 2 + len(serializedContents)
	copy(resp[marker:marker+16], t.TopicID[:])
	marker += 16
	resp[marker] = SerializeBool(t.IsInternal)
	marker++
	partSize := int(t.Partitions.Size())
	serPart := t.Partitions.Serialize()
	copy(resp[marker:marker+partSize], serPart)
	marker += partSize

	binary.BigEndian.PutUint32(resp[marker:], t.TopicAuthorizedOperations)
	return resp

}

func (t *Topic) SerializePartitions() []byte {
	numPartitions := len(t.Partitions)
	if numPartitions == 0 {
		return []byte{1}
	}
	lenSpace := UvarintSpace(uint64(numPartitions + 1))

	resp := make([]byte, lenSpace+int(numPartitions)*PartitionLen)
	binary.PutUvarint(resp[0:lenSpace], uint64(numPartitions+1))
	marker := lenSpace
	for _, v := range t.Partitions {
		copy(resp[marker:PartitionLen], v.Serialize())
		marker += PartitionLen
	}
	return resp

}

func (t *Topic) Size() uint32 {
	return 24 + t.Contents.Size() + t.Partitions.Size()
}

func (p *Partition) Serialize() []byte {
	resp := make([]byte, PartitionLen)
	binary.BigEndian.PutUint16(resp[0:2], p.ErrorCode)
	binary.BigEndian.PutUint32(resp[2:6], p.PartitionIndex)
	binary.BigEndian.PutUint32(resp[6:10], p.LeaderID)
	binary.BigEndian.PutUint32(resp[10:14], p.LeaderEpoch)
	binary.BigEndian.PutUint32(resp[14:18], p.ReplicaNodes)
	binary.BigEndian.PutUint32(resp[18:22], p.ISRNodes)
	binary.BigEndian.PutUint32(resp[22:26], p.EligibleLeaderReplicas)
	binary.BigEndian.PutUint32(resp[26:30], p.LastKnownELR)
	binary.BigEndian.PutUint32(resp[30:34], p.OfflineReplicas)
	// last byte is a tag buffer

	return resp

}

func (p *Partition) Size() uint32 {
	return PartitionLen
}

func (c *Cursor) Serialize() []byte {
	resp := make([]byte, c.Size())
	if c == nil {
		resp[0] = 255
	}
	return resp

	//lenSpace := UvarintSpace(uint64(c.Size()))

}

func (c *Cursor) Size() uint32 {
	if c == nil {
		return 1
	}
	return c.TopicName.Size() + 5
}
