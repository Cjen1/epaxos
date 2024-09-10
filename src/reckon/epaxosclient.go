package reckon

import (
	"bufio"
	"epaxos/genericsmrproto"
	"epaxos/masterproto"
	"epaxos/state"
	"hash/fnv"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"strconv"
	"sync"
)

type Config struct {
	MasterAddr   string
	ClientNumber int32
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
	inflight sync.Map
	addrs    []string
	writers  sync.Map
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
		res, ok := c.writers.Load(addr)
		if ok {
			return res.(*SafeWriter)
		}
	}
}

func (c *Client) submit(args genericsmrproto.Propose) *outstandingReq {
	args.CommandId = c.idgen.next_id(c.config.ClientNumber)

	ir := outstandingReq{
		result: make(chan string),
	}

	log.Println("Submitting req")

	writer := c.random_writer()
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.writer.WriteByte(genericsmrproto.PROPOSE)
	args.Marshal(writer.writer)
	writer.writer.Flush()

	log.Println("Request submitted")

	c.inflight.Store(args.CommandId, &ir)

	return &ir
}

func conv_str(s string) int64 {
	res, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return res
	}
	h := fnv.New64a()
	h.Write([]byte(s))
	return int64(h.Sum64())
}

func (c *Client) Put(k string, v string) (string, error) {
	args := genericsmrproto.Propose{0, state.Command{state.PUT, state.Key(conv_str(k)), state.Value(conv_str(v))}, 0}
	<-c.submit(args).result
	return "unknown", nil
}

func (c *Client) Get(k string) (string, string, error) {
	args := genericsmrproto.Propose{0, state.Command{state.GET, state.Key(conv_str(k)), 0}, 0}
	<-c.submit(args).result
	return "", "unknown", nil
}

func (c *Client) Close() {
	// TODO
}

func dial_master(cfg Config) *rpc.Client {
	log.Println("Dialing master")
	var master *rpc.Client
	var err error
	for {
		master, err = rpc.DialHTTP("tcp", cfg.MasterAddr)
		if err != nil {
			log.Println("Error connecting to master", err)
		} else {
			return master
		}
	}
}

func get_replica_list(cfg Config) []string {
	log.Println("Getting replica list")
	master := dial_master(cfg)
	defer master.Close()
	rlReply := new(masterproto.GetReplicaListReply)
	for !rlReply.Ready {
		err := master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
		if err != nil {
			log.Println("Error making the GetReplicaList RPC", err)
		}
	}
	log.Println("Got replicas: ", rlReply.ReplicaList)
	return rlReply.ReplicaList
}

func (cli *Client) dial_replica(addr string) {
	log.Println("Dialling replica: ", addr)
	for !cli.close {
		server, err := net.Dial("tcp", addr)
		if err != nil {
			log.Fatalf("Error connecting to replica %s\n", addr)
		}
		reader := bufio.NewReader(server)
		writer := bufio.NewWriter(server)

		log.Println("Dial success: ", addr)
		cli.mu.Lock()
		cli.writers.Store(addr, &SafeWriter{
			mu:     sync.Mutex{},
			writer: writer,
		})
		cli.mu.Unlock()

		var reply genericsmrproto.ProposeReply
		for !cli.close {
			if err := reply.Unmarshal(reader); err != nil || reply.OK == 0 {
				log.Println("Error when reading:", addr, err)
				break
			}

			log.Println("Response received: ", reply)

			l, ok := cli.inflight.Load(reply.CommandId)

			ir := l.(*outstandingReq)

			if ok {
				ir.result <- "success"
			}
		}

		cli.mu.Lock()
		cli.writers.Delete(addr)
		cli.mu.Unlock()
		server.Close()
	}
}

func NewClient(cfg Config) *Client {
	addrs := get_replica_list(cfg)
	cli := Client{
		config:   cfg,
		mu:       sync.Mutex{},
		idgen:    SafeIdGen{mu: sync.Mutex{}, i: 0},
		inflight: sync.Map{},
		addrs:    addrs,
		writers:  sync.Map{},
		close:    false,
	}

	for _, addr := range cli.addrs {
		go cli.dial_replica(addr)
	}

	return &cli
}
