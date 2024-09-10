package main

import (
	"epaxos/reckon"
	"flag"
  "strconv"

  rc_go "github.com/Cjen1/reckon/reckon/goclient"
)

var masterAddr *string = flag.String("maddr", "localhost:5001", "MasterAddress defaults to localhost:5001")
var clientId *int = flag.Int("id", 0, "Client Id.")
var ncpr *bool = flag.Bool("ncpr", false, "New client per request.")

func main() {
  flag.Parse()

  config := reckon.Config{
  	MasterAddr:   *masterAddr,
  	ClientNumber: int32(*clientId),
  }

  gen_cli := func() (rc_go.Client, error) {
    return reckon.NewClient(config), nil
    }
  rc_go.Run(gen_cli, strconv.Itoa(*clientId), *ncpr)
}
