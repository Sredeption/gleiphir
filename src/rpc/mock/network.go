package mock

import (
	"math/rand"
	"sync"
	"time"
)

type Network struct {
	mu             sync.Mutex
	reliable       bool
	longDelays     bool                        // pause a long time on send on disabled connection
	longReordering bool                        // sometimes delay replies a long time
	ends           map[interface{}]*ClientEnd  // ends, by name
	enabled        map[interface{}]bool        // by end name
	servers        map[interface{}]*Server     // servers, by name
	connections    map[interface{}]interface{} // endName -> serverName
	endCh          chan reqMsg
}

func MakeNetwork() *Network {
	network := &Network{}
	network.reliable = true
	network.ends = map[interface{}]*ClientEnd{}
	network.enabled = map[interface{}]bool{}
	network.servers = map[interface{}]*Server{}
	network.connections = map[interface{}]interface{}{}
	network.endCh = make(chan reqMsg)

	// single goroutine to handle all ClientEnd.Call()s
	go func() {
		for xReq := range network.endCh {
			go network.ProcessReq(xReq)
		}
	}()

	return network
}

func (n *Network) Reliable(yes bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.reliable = yes
}

func (n *Network) LongReordering(yes bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.longReordering = yes
}

func (n *Network) LongDelays(yes bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.longDelays = yes
}

func (n *Network) ReadEndNameInfo(endName interface{}) (enabled bool,
	serverName interface{}, server *Server, reliable bool, longReordering bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	enabled = n.enabled[endName]
	serverName = n.connections[endName]
	if serverName != nil {
		server = n.servers[serverName]
	}
	reliable = n.reliable
	longReordering = n.longReordering
	return
}

func (n *Network) IsServerDead(endName interface{}, serverName interface{}, server *Server) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.enabled[endName] == false || n.servers[serverName] != server {
		return true
	}
	return false
}

func (n *Network) ProcessReq(req reqMsg) {
	enabled, serverName, server, reliable, longReordering := n.ReadEndNameInfo(req.endName)

	if enabled && serverName != nil && server != nil {
		if reliable == false {
			// short delay
			ms := rand.Int() % 27
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if reliable == false && (rand.Int()%1000) < 100 {
			// drop the request, return as if timeout
			req.replyCh <- replyMsg{false, nil}
			return
		}

		// execute the request (call the RPC handler).
		// in a separate thread so that we can periodically check
		// if the server has been killed and the RPC should get a
		// failure reply.
		ech := make(chan replyMsg)
		go func() {
			r := server.dispatch(req)
			ech <- r
		}()

		// wait for handler to return,
		// but stop waiting if DeleteServer() has been called,
		// and return an error.
		var reply replyMsg
		replyOK := false
		serverDead := false
		for replyOK == false && serverDead == false {
			select {
			case reply = <-ech:
				replyOK = true
			case <-time.After(100 * time.Millisecond):
				serverDead = n.IsServerDead(req.endName, serverName, server)
			}
		}

		// do not reply if DeleteServer() has been called, i.e.
		// the server has been killed. this is needed to avoid
		// situation in which a client gets a positive reply
		// to an Append, but the server persisted the update
		// into the old Persister. config.go is careful to call
		// DeleteServer() before superseding the Persister.
		serverDead = n.IsServerDead(req.endName, serverName, server)

		if replyOK == false || serverDead == true {
			// server was killed while we were waiting; return error.
			req.replyCh <- replyMsg{false, nil}
		} else if reliable == false && (rand.Int()%1000) < 100 {
			// drop the reply, return as if timeout
			req.replyCh <- replyMsg{false, nil}
		} else if longReordering == true && rand.Intn(900) < 600 {
			// delay the response for a while
			ms := 200 + rand.Intn(1+rand.Intn(2000))
			time.Sleep(time.Duration(ms) * time.Millisecond)
			req.replyCh <- reply
		} else {
			req.replyCh <- reply
		}
	} else {
		// simulate no reply and eventual timeout.
		ms := 0
		if n.longDelays {
			// let Raft tests check that leader doesn't send
			// RPCs synchronously.
			ms = rand.Int() % 7000
		} else {
			// many kv tests require the client to try each
			// server in fairly rapid succession.
			ms = rand.Int() % 100
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
		req.replyCh <- replyMsg{false, nil}
	}

}

// create a client end-point.
// start the thread that listens and delivers.
func (n *Network) MakeEnd(endName interface{}) *ClientEnd {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, ok := n.ends[endName]; ok {
		logger.Fatalf("MakeEnd: %v already exists\n", endName)
	}

	e := &ClientEnd{}
	e.endName = endName
	e.ch = n.endCh
	n.ends[endName] = e
	n.enabled[endName] = false
	n.connections[endName] = nil

	return e
}

func (n *Network) AddServer(serverName interface{}, rs *Server) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.servers[serverName] = rs
}

func (n *Network) DeleteServer(serverName interface{}) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.servers[serverName] = nil
}

// connect a ClientEnd to a server.
// a ClientEnd can only be connected once in its lifetime.
func (n *Network) Connect(endName interface{}, serverName interface{}) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.connections[endName] = serverName
}

// enable/disable a ClientEnd.
func (n *Network) Enable(endName interface{}, enabled bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.enabled[endName] = enabled
}

// get a server's count of incoming RPCs.
func (n *Network) GetCount(serverName interface{}) int {
	n.mu.Lock()
	defer n.mu.Unlock()

	svr := n.servers[serverName]
	return svr.GetCount()
}
