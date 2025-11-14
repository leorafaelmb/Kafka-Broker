package main

import protocol2 "github.com/codecrafters-io/kafka-starter-go/protocol"

func run(path string) error {
	broker := protocol2.NewBroker()
	broker.Startup()
	return nil
}
