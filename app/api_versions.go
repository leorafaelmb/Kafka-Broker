package main

import (
	"encoding/binary"
	"io"
)

type ApiVersionsRequest struct {
	ClientSoftwareName    string
	ClientSoftwareVersion string
}

type ApiVersionsResponse struct {
	ErrorCode      int16
	ApiVersions    []ApiVersion
	ThrottleTimeMs int32
}

type ApiVersion struct {
	APIKey     uint16
	MinVersion uint16
	MaxVersion uint16
}

func deserializeApiVersions(rbr ReaderByteReader) (*ApiVersionsRequest, error) {
	clientSoftwareName, err := ReadCompactString(rbr)
	if err != nil {
		return nil, err
	}

	clientSoftwareVersion, err := ReadCompactString(rbr)
	if err != nil {
		return nil, err
	}

	return &ApiVersionsRequest{
		ClientSoftwareName:    clientSoftwareName,
		ClientSoftwareVersion: clientSoftwareVersion,
	}, nil
}

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

func (k ApiVersion) Serialize() []byte {
	resp := make([]byte, k.Len())
	binary.BigEndian.PutUint16(resp[0:2], k.APIKey)
	binary.BigEndian.PutUint16(resp[2:4], k.MinVersion)
	binary.BigEndian.PutUint16(resp[4:6], k.MaxVersion)
	// Tag field buffer at the end, so resp[6] == 0

	return resp

}

func (r ApiVersionsResponse) Serialize() []byte {
	resp := make([]byte, 0, r.Len())

	resp = binary.BigEndian.AppendUint16(resp, uint16(r.ErrorCode))
	resp = binary.AppendUvarint(resp, uint64(len(r.ApiVersions)+1))

	for _, key := range r.ApiVersions {
		resp = append(resp, key.Serialize()...)
	}
	resp = binary.BigEndian.AppendUint32(resp, uint32(r.ThrottleTimeMs))

	// tag buffer
	resp = append(resp, 0)

	return resp
}

func (k ApiVersion) Len() uint32 {
	return 7
}

func (r ApiVersionsResponse) Len() uint32 {
	// error code, throttle time, array length, and tag buffer
	var length uint32 = 6

	for _, k := range r.ApiVersions {
		length += k.Len()
	}

	return length

}
