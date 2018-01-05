package native

import (
	"github.com/op/go-logging"
	"rpc"
)

var logger = logging.MustGetLogger("rpc/native")

type Peers struct {
	Ends []*ClientEnd
}

func (p *Peers) Len() int {
	return len(p.Ends)
}

func (p *Peers) GetEnd(i int) rpc.ClientEnd {
	return p.Ends[i]
}

func Setup(addresses map[string]string, makeService func(config rpc.Peers) []interface{}) *Peers {
	config := new(Peers)
	network := MakeNetwork()
	config.Ends = make([]*ClientEnd, len(addresses))
	services := makeService(config)
	i := 0
	for serverName, address := range addresses {
		endName := "end-" + serverName
		config.Ends[i] = network.MakeEnd(endName)
		server := MakeServer(address)
		service := MakeService(services[i])
		server.AddService(service)
		network.AddServer(serverName, server)
		network.Connect(endName, serverName)
		i++
	}

	return config
}
