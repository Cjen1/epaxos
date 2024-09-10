package main

import (
	"epaxos/reckon"
  "log"
	"flag"
)

var masterAddr *string = flag.String("maddr", "localhost:5001", "MasterAddress defaults to localhost:5001")

func main() {
  flag.Parse()

  config := reckon.Config{
  	MasterAddr:   *masterAddr,
  	ClientNumber: int32(0),
  }

  cli := reckon.NewClient(config)
  log.Println("Sending rpc")
  cli.Put("test", "2")
  cli.Get("test")
  log.Println("Rpc returned")
}
