package protocol

import (
	"encoding/binary"
)

type ApiVersionsRequest struct {
	ClientSoftwareName    string
	ClientSoftwareVersion string
}

type ApiVersionsResponse struct {
	ErrorCode      int16
	APIKeys        []APIKey
	ThrottleTimeMs int32
}

type APIKey struct {
	APIKey     uint16
	MinVersion uint16
	MaxVersion uint16
}

const ApiVersionLength = 7

func getAPIVersionsResponse(header RequestHeader) []byte {
	versions := []APIKey{
		{
			APIKey:     ApiVersionsKey,
			MinVersion: ApiVersionsMinVersion,
			MaxVersion: ApiVersionsMaxVersion,
		},
		{
			APIKey:     DescribeTopicPartitionsKey,
			MinVersion: DescribeTopicPartitionsMinVersion,
			MaxVersion: DescribeTopicPartitionsMaxVersion,
		},
	}
	var errorCode int16 = 0
	if header.RequestAPIVersion < 0 || header.RequestAPIVersion > 4 {
		errorCode = 35
	}

	av := ApiVersionsResponse{
		ErrorCode:      errorCode,
		APIKeys:        versions,
		ThrottleTimeMs: 0,
	}

	avResp := av.Serialize()

	msgSize := 4 + len(avResp)
	resp := make([]byte, 4+msgSize)

	binary.BigEndian.PutUint32(resp[0:4], uint32(msgSize))
	binary.BigEndian.PutUint32(resp[4:8], uint32(header.CorrelationID))
	copy(resp[8:], avResp)

	return resp
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

func (k APIKey) serialize() []byte {
	resp := make([]byte, ApiVersionLength)
	binary.BigEndian.PutUint16(resp[0:2], k.APIKey)
	binary.BigEndian.PutUint16(resp[2:4], k.MinVersion)
	binary.BigEndian.PutUint16(resp[4:6], k.MaxVersion)
	// Tag field buffer at the end, so resp[6] == 0

	return resp

}

func (r ApiVersionsResponse) Serialize() []byte {
	resp := make([]byte, 0, r.Len())

	resp = binary.BigEndian.AppendUint16(resp, uint16(r.ErrorCode))
	resp = binary.AppendUvarint(resp, uint64(len(r.APIKeys)+1))

	for _, key := range r.APIKeys {
		resp = append(resp, key.serialize()...)
	}

	resp = binary.BigEndian.AppendUint32(resp, uint32(r.ThrottleTimeMs))

	// tag buffer
	resp = append(resp, 0)

	return resp
}

func (r ApiVersionsResponse) Len() uint32 {
	// error code, throttle time, array length, and tag buffer all have length 6

	return uint32(6 + ApiVersionLength*len(r.APIKeys))
}
