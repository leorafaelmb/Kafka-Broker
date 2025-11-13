package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	msgSizeBuf := make([]byte, 4)
	if _, err := conn.Read(msgSizeBuf); err != nil {
		return
	}
	msgSize := binary.BigEndian.Uint32(msgSizeBuf)

	reqBuf := make([]byte, msgSize)
	if _, err := conn.Read(reqBuf); err != nil {
		return
	}

	connReader := bytes.NewBuffer(reqBuf)

	resp, err := createResponse(connReader)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	_, err = conn.Write(resp)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

}

type ReaderByteReader interface {
	io.Reader
	io.ByteReader
}

func createResponse(connReader ReaderByteReader) ([]byte, error) {
	requestHeader, err := deserializeRequestHeader(connReader)
	if err != nil {
		return nil, nil
	}

	switch requestHeader.RequestAPIKey {
	case ApiVersions:
		_, err := deserializeApiVersions(connReader)
		if err != nil {
			return nil, err
		}

		var r ApiVersionsResponse
		version := int16(requestHeader.RequestAPIVersion)
		if version >= 0 && version <= 4 {
			r.ErrorCode = 0
		} else {
			r.ErrorCode = 35
		}

		apiResp := r.Serialize()
		msgSize := 4 + len(apiResp)
		resp := make([]byte, msgSize)

		binary.BigEndian.PutUint32(resp[0:4], uint32(msgSize))
		binary.BigEndian.PutUint32(resp[4:8], requestHeader.CorrelationID)
		copy(resp[8:], apiResp)
		return resp, nil

	}
	return nil, nil
}
