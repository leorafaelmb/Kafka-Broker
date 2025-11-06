package main

import (
	"encoding/binary"
	"fmt"
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
	buf := make([]byte, 1024)
	_, err := conn.Read(buf)
	if err != nil {
		fmt.Println("error reading from connection: ", err.Error())
		return
	}

	rh := parseRequestHeaderV2(buf)
	resp := make([]byte, 8)
	binary.BigEndian.PutUint32(resp[4:8], uint32(rh.CorrelationID))
	_, err = conn.Write(resp)
	if err != nil {
		fmt.Println("error writing to connection: ", err.Error())
		return
	}

}

type RequestHeaderV2 struct {
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
	ClientID          string
}

func parseRequestHeaderV2(request []byte) *RequestHeaderV2 {
	//messageSize := binary.BigEndian.Uint32(request[0:4])
	requestAPIKey := int16(binary.BigEndian.Uint16(request[4:6]))
	requestAPIVersion := int16(binary.BigEndian.Uint16(request[6:8]))
	correlationID := int32(binary.BigEndian.Uint32(request[8:12]))
	return &RequestHeaderV2{
		RequestAPIKey:     requestAPIKey,
		RequestAPIVersion: requestAPIVersion,
		CorrelationID:     correlationID,
	}
}
