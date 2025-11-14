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
		msgSizeBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, msgSizeBuf); err != nil {
			fmt.Println(err.Error())
			return
		}
		msgSize := binary.BigEndian.Uint32(msgSizeBuf)

		reqBuf := make([]byte, msgSize)
		if _, err := conn.Read(reqBuf); err != nil {
			fmt.Println(err.Error())
			return
		}

		connReader := bytes.NewBuffer(reqBuf)
		header, err := deserializeRequestHeader(connReader)
		if err != nil {
			fmt.Println(err)
			return
		}

		disp := b.APIDispatcher(header.RequestAPIKey)
		resp := disp.Handler(*header)

		_, err = conn.Write(resp)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	}

}
