package native

import "sync"

type Network struct {
	mu          sync.Mutex
	ends        map[interface{}]*ClientEnd  // ends, by name
	servers     map[interface{}]*Server     // servers, by name
	connections map[interface{}]interface{} // endName -> serverName
}

func MakeNetwork() *Network {
	network := new(Network)

	network.ends = map[interface{}]*ClientEnd{}
	network.servers = map[interface{}]*Server{}
	network.connections = map[interface{}]interface{}{}
	return network
}

func (n *Network) AddServer(serverName interface{}, server *Server) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.servers[serverName] = server
}

func (n *Network) MakeEnd(endName interface{}) *ClientEnd {
	e := new(ClientEnd)
	if _, ok := n.ends[endName]; ok {
		logger.Criticalf("MakeEnd: %v already exists\n", endName)
	}
	e.endName = endName
	n.ends[endName] = e
	n.connections[endName] = nil
	return e
}

func (n *Network) Connect(endName interface{}, serverName interface{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connections[endName] = serverName
	n.ends[endName].address = n.servers[serverName].address
}
