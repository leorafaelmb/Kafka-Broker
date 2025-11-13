package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

type ApiVersionsRequest struct {
	ClientSoftwareName    string
	ClientSoftwareVersion string
}

type ApiVersionsResponse struct {
	ErrorCode      int16
	APIKey         int16
	MinVersion     int16
	MaxVersion     int16
	ThrottleTimeMs int32
}

func deserializeApiVersions(rbr ReaderByteReader) (*ApiVersionsRequest, error) {
	clientSoftwareName, err := ReadCompactString(rbr)
	if err != nil {
		fmt.Println(err.Error())
		return nil, nil
	}

	clientSoftwareVersion, err := ReadCompactString(rbr)
	if err != nil {
		fmt.Println(err.Error())
		return nil, nil
	}

	return &ApiVersionsRequest{
		ClientSoftwareName:    clientSoftwareName,
		ClientSoftwareVersion: clientSoftwareVersion,
	}, nil
}

func ReadCompactString(rbr ReaderByteReader) (string, error) {
	length, err := binary.ReadUvarint(rbr)
	if err != nil {
		return "", nil
	}
	buf := make([]byte, length-1)
	if _, err = io.ReadFull(rbr, buf); err != nil {
		return "", err
	}

	return string(buf), nil

}

func (r ApiVersionsResponse) Serialize() []byte {
	resp := make([]byte, 12)
	binary.BigEndian.PutUint16(resp[0:2], uint16(r.ErrorCode))
	binary.BigEndian.PutUint16(resp[2:4], uint16(r.APIKey))
	binary.BigEndian.PutUint16(resp[4:6], uint16(r.MinVersion))
	binary.BigEndian.PutUint16(resp[6:8], uint16(r.MaxVersion))
	binary.BigEndian.PutUint32(resp[8:12], uint32(r.ThrottleTimeMs))

	return resp
}

func (r ApiVersionsResponse) Len() int32 {
	return 12
}
