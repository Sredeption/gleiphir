package main

import (
	"data"
	"github.com/op/go-logging"
	"os"
	"raft"
	"rpc/native"
	"strings"
)

var logger = logging.MustGetLogger("main")

func main() {
	logging.SetLevel(logging.INFO, "rpc/native")
	gleiphirId := os.Getenv("GLEIPHIR_ID")
	gleiphirServers := os.Getenv("GLEIPHIR_SERVERS")

	logger.Infof("Received GLEIPHIR_ID=%s", gleiphirId)
	var me int

	var serverNames []string
	var addresses []string

	for index, s := range strings.Split(gleiphirServers, " ") {
		p := strings.Split(s, "=")
		serverNames = append(serverNames, p[0])
		addresses = append(addresses, p[1])
		if p[0] == gleiphirId {
			me = index
		}
	}
	num := len(serverNames)
	logger.Infof("Received GLEIPHIR_SERVERS:%+v", addresses)

	applyCh := make(chan raft.ApplyMsg)
	persister := data.MakeMemoryPersister()

	// initiate native rpc
	peers := new(native.Peers)
	network := native.MakeNetwork()
	peers.Ends = make([]*native.ClientEnd, num)

	for i := 0; i < num; i++ {
		endName := "end-" + serverNames[i]
		peers.Ends[i] = network.MakeEnd(endName)
	}

	var localServer *native.Server
	rf := raft.Make(peers, me, persister, applyCh)
	for i := 0; i < num; i++ {
		endName := "end-" + serverNames[i]
		server := native.MakeServer(addresses[i])
		if i == me {
			localServer = server
			service := native.MakeService(rf)
			server.AddService(service)
		}
		network.AddServer(serverNames[i], server)
		network.Connect(endName, serverNames[i])
	}

	localServer.Start()
}
