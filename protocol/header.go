package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

type RequestHeader struct {
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
	ClientID          string
}

func deserializeRequestHeader(connReader ReaderByteReader) (*RequestHeader, error) {
	var header RequestHeader

	if err := binary.Read(connReader, binary.BigEndian, &header.RequestAPIKey); err != nil {
		fmt.Println("error reading request api key")
		return nil, err
	}
	if err := binary.Read(connReader, binary.BigEndian, &header.RequestAPIVersion); err != nil {
		fmt.Println("error reading request api version")
		return nil, err
	}
	if err := binary.Read(connReader, binary.BigEndian, &header.CorrelationID); err != nil {
		fmt.Println("error reading correlation id")
		return nil, err
	}

	var clientIDLength int16
	if err := binary.Read(connReader, binary.BigEndian, &clientIDLength); err != nil {
		fmt.Println("error reading clientID length")
		return nil, err
	}
	if clientIDLength > -1 {
		buf := make([]byte, clientIDLength)
		if _, err := io.ReadFull(connReader, buf); err != nil {
			fmt.Printf("error reading client ID: %v\n", err.Error())
			return nil, err
		}
		header.ClientID = string(buf)
	}

	numTaggedFields, err := binary.ReadUvarint(connReader)
	if err != nil {
		return nil, err
	}

	// skip over tagged fields - discard for now
	if _, err = io.CopyN(io.Discard, connReader, int64(numTaggedFields)); err != nil {
		return nil, err
	}
	return &header, nil
}
