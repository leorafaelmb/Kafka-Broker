package protocol

import (
	"encoding/binary"
	"io"
)

type Serializeable interface {
	Serialize() []byte
	Size() uint32
}

type CompactString string
type CompactArray[T Serializeable] []T

func ReadCompactString(rbr ReaderByteReader) (CompactString, error) {
	length, err := binary.ReadUvarint(rbr)
	if err != nil {
		return "", err
	}

	buf := make([]byte, length-1)
	if _, err = io.ReadFull(rbr, buf); err != nil {
		return "", err
	}

	return CompactString(buf), nil

}

func (c CompactString) Serialize() []byte {
	n := len(c) + 1
	lenSpace := UvarintSpace(uint64(n))
	cs := make([]byte, lenSpace+len(c))
	binary.PutUvarint(cs[0:lenSpace], uint64(n))
	copy(cs[lenSpace:], c)
	return cs
}

func (c CompactString) Size() uint32 {
	lenSpace := UvarintSpace(uint64(len(c) + 1))
	return uint32(lenSpace + len(c))
}

func (c *CompactArray[T]) Serialize() []byte {
	if c == nil {
		return []byte{0}
	} else if len(*c)+1 == 0 {
		return []byte{1}
	}

	numItems := len(*c)
	lenSpace := UvarintSpace(uint64(numItems + 1))
	resp := make([]byte, c.Size())
	binary.PutUvarint(resp, uint64(numItems+1))
	var offset = uint32(lenSpace)
	for _, v := range *c {
		copy(resp[offset:offset+v.Size()], v.Serialize())
		offset += v.Size()
	}

	return resp
}

func (c *CompactArray[T]) Size() uint32 {
	if c == nil {
		return 0
	} else if len(*c) == 0 {
		return 1
	}
	var totalSize = uint32(UvarintSpace(uint64(len(*c) + 1)))
	for _, v := range *c {
		totalSize += v.Size()
	}

	return totalSize
}

// Compact arrays can also hold structures... will account for later.
func ReadCompactArray(rbr ReaderByteReader) ([]CompactString, error) {
	n, err := binary.ReadUvarint(rbr)
	if err != nil {
		return nil, err
	}

	arr := make([]CompactString, n-1)

	var i uint64
	for i = 0; i < n-1; i++ {
		arr[i], err = ReadCompactString(rbr)
		if err != nil {
			return nil, err
		}
		//discard field tags
		if err = DiscardTaggedFields(rbr); err != nil {
			return nil, err
		}
	}

	return arr, nil

}

func DiscardTaggedFields(rbr ReaderByteReader) error {
	numTaggedFields, err := binary.ReadUvarint(rbr)
	if err != nil {
		return err
	}

	// skip over tagged fields - discard for now
	if _, err = io.CopyN(io.Discard, rbr, int64(numTaggedFields)); err != nil {
		return err
	}
	return nil
}

func SerializeBool(v bool) byte {
	if v {
		return 1
	}
	return 0
}

func UvarintSpace(x uint64) int {
	n := 0
	for x != 0 {
		x = x >> 8
		n++
	}
	return n
}
