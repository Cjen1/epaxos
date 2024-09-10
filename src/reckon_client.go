package main

import (
	"bufio"
	"epaxos/genericsmrproto"
	"epaxos/masterproto"
	"epaxos/state"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"sync"

	rc_go "github.com/cjen1/reckon/reckon/goclient"
)

var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port.")
var clientId *int = flag.Int("cn", 0, "Client ID.")
var newClientPerRequest *bool = flag.Bool("ncpr", false, "Use a new client per request.")

type Config struct {
	masterAddr   string
	masterPort   int
	clientNumber int32
}

type outstandingReq struct {
	result chan string
}

type SafeWriter struct {
	mu     sync.Mutex
	writer *bufio.Writer
}

type SafeIdGen struct {
	mu sync.Mutex
	i  int32
}

type Client struct {
	config   Config
	mu       sync.Mutex
	idgen    SafeIdGen
	inflight map[int32]*outstandingReq
	addrs    []string
	writers  map[string]*SafeWriter
	close    bool
}

func (i *SafeIdGen) next_id(clientNumber int32) int32 {
	base := (math.MaxInt32 / 8) * clientNumber
	i.mu.Lock()
	res := base + i.i
	i.i += 1
	i.mu.Unlock()
	return res
}

func (c *Client) random_writer() *SafeWriter {
	for {
		addr := c.addrs[rand.Intn(len(c.addrs))]
		res := c.writers[addr]
		if res != nil {
			return res
		}
	}
}

func (c *Client) submit(args genericsmrproto.Propose) *outstandingReq {
	args.CommandId = c.idgen.next_id(c.config.clientNumber)

	ir := outstandingReq{}

	writer := c.random_writer()
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.writer.WriteByte(genericsmrproto.PROPOSE)
	args.Marshal(writer.writer)
	writer.writer.Flush()

	c.inflight[args.CommandId] = &ir

	return &ir
}

func (c *Client) Put(k string, v string) (string, error) {
	args := genericsmrproto.Propose{c.idgen.next_id(), state.Command{state.PUT, 0, 0}, 0}
	res := <-c.submit(args).result
	return "unknown", nil
}

func (c *Client) Get(k string) (string, string, error) {
	args := genericsmrproto.Propose{c.idgen.next_id(), state.Command{state.GET, 0, 0}, 0}
	res := <-c.submit(args).result
	return "", "unknown", nil
}

func (c *Client) Close() {
	// TODO
}

func dial_master(cfg Config) *rpc.Client {
	var master *rpc.Client
	var err error
	for {
		master, err = rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *masterAddr, *masterPort))
		if err != nil {
			log.Println("Error connecting to master", err)
		} else {
			return master
		}
	}
}

func get_replica_list(cfg Config) []string {
	master := dial_master(cfg)
	defer master.Close()
	rlReply := new(masterproto.GetReplicaListReply)
	for !rlReply.Ready {
		err := master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
		if err != nil {
			log.Println("Error making the GetReplicaList RPC", err)
		}
	}
	return rlReply.ReplicaList
}

func (cli *Client) dial_replica(addr string) {
	for !cli.close {
		server, err := net.Dial("tcp", addr)
		if err != nil {
			log.Fatalf("Error connecting to replica %d\n", addr)
		}
		reader := bufio.NewReader(server)
		writer := bufio.NewWriter(server)

		cli.mu.Lock()
		cli.writers[addr] = &SafeWriter{writer: writer}
		cli.mu.Unlock()

		var reply genericsmrproto.ProposeReply
		for !cli.close {
			if err := reply.Unmarshal(reader); err != nil || reply.OK == 0 {
				log.Println("Error when reading:", addr, err)
				break
			}

			ir := cli.inflight[reply.CommandId]

			if ir != nil {
				ir.result <- "Success"
			}
		}

		cli.mu.Lock()
		delete(cli.writers, addr)
		cli.mu.Unlock()
		server.Close()
	}
}

func new_client(cfg Config) *Client {
  addrs := get_replica_list(cfg)
  cli := Client{
  	config:   cfg,
  	addrs:    addrs,
  	close:    false,
  }

  for _, addr := range(cli.addrs) {
    cli.dial_replica(addr)
  }

  return &cli
}

func main() {
	flag.Parse()

	flag.Parse()

  config := Config{
  	masterAddr:   *masterAddr,
  	masterPort:   *masterPort,
  	clientNumber: int32(*clientId),
  }

  rc_go.Run(func() (rc_go.Client, error){
    return new_client(config), nil
  }, *clientId, *newClientPerRequest)
}
