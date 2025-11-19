package protocol

import (
	"encoding/binary"
	"io"
)

func ReadCompactString(rbr ReaderByteReader) (string, error) {
	length, err := binary.ReadUvarint(rbr)
	if err != nil {
		return "", err
	}

	buf := make([]byte, length-1)
	if _, err = io.ReadFull(rbr, buf); err != nil {
		return "", err
	}

	return string(buf), nil

}

// Compact arrays can also hold structures... will account for later.
func ReadCompactArray(rbr ReaderByteReader) ([]string, error) {
	n, err := binary.ReadVarint(rbr)
	if err != nil {
		return nil, err
	}

	arr := make([]string, n-1)

	var i int64
	for i = 0; i < n-1; i++ {
		s, err := ReadCompactString(rbr)
		if err != nil {
			return nil, err
		}
		//discard field tags
		if err = DiscardTaggedFields(rbr); err != nil {
			return nil, err
		}
		arr[i] = s
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
