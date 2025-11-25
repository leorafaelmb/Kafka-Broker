package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

type Broker struct {
}

func NewBroker() *Broker {
	return &Broker{}
}

func (b *Broker) Startup() {
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

		go b.HandleConnection(conn)
	}
}

func (b *Broker) HandleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		var msgSize uint32
		if err := binary.Read(conn, binary.BigEndian, &msgSize); err != nil {
			fmt.Println(err.Error())
		}
		reqBuf := make([]byte, msgSize)
		if _, err := io.ReadFull(conn, reqBuf); err != nil {
			fmt.Println(err.Error())
			return
		}

		connReader := bytes.NewBuffer(reqBuf)
		header, err := deserializeRequestHeader(connReader)
		if err != nil {
			fmt.Println(err)
			return
		}

		header.Body = connReader.Bytes()

		disp := b.APIDispatcher(header.RequestAPIKey)
		resp := disp.Handler(*header)

		_, err = conn.Write(resp)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	}

}
