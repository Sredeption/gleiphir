package main

import (
	"data"
	"github.com/op/go-logging"
	"os"
	"rpc/native"
	"strings"
	"raft"
)

var logger = logging.MustGetLogger("main")

func main() {
	addresses := make(map[string]string)
	gleiphirId := os.Getenv("GLEIPHIR_ID")
	gleiphirServers := os.Getenv("GLEIPHIR_SERVERS")

	logger.Infof("Received GLEIPHIR_ID=%s", gleiphirId)
	logger.Infof("Received GLEIPHIR_SERVERS=%s", gleiphirServers)

	for _, s := range strings.Split(gleiphirServers, " ") {
		p := strings.Split(s, "=")
		addresses[p[0]] = p[1]
	}

	config := new(native.Peers)
	network := native.MakeNetwork()
	config.Ends = make([]*native.ClientEnd, len(addresses))
	var z *raft.Raft

	i := 0
	applyCh := make(chan raft.ApplyMsg)
	persister := data.MakeMemoryPersister()
	for id, address := range addresses {
		endName := "end-" + id
		config.Ends[i] = network.MakeEnd(endName)
		server := native.MakeServer(address)
		if id == gleiphirId {
			z = raft.Make(config, i, persister, applyCh)
			service := native.MakeService(z)
			server.AddService(service)
		}
		network.AddServer(id, server)
		network.Connect(endName, id)
		i++
	}

}
