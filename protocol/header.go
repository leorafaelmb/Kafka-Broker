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
	Body              []byte
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

	if err := DiscardTaggedFields(connReader); err != nil {
		return nil, err
	}

	return &header, nil
}
