package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal(fmt.Errorf("not enough arguments"))
	}
	if err := run(os.Args[1]); err != nil {
		log.Fatal(err)
	}
}
