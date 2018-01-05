package mock

//
// channel-based RPC, for 824 labs.
//
// simulates a network that can lose requests, lose replies,
// delay messages, and entirely disconnect particular hosts.
//
// we will use the original rpc_mock.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test against the original before submitting.
//
// adapted from Go net/rpc/server.go.
//
// sends gob-encoded values to ensure that RPCs
// don't include references to program objects.
//
// net := MakeNetwork() -- holds network, clients, servers.
// end := net.MakeEnd(endName) -- create a client end-point, to talk to one server.
// net.AddServer(serverName, server) -- adds a named server to network.
// net.DeleteServer(serverName) -- eliminate the named server.
// net.Connect(endName, serverName) -- connect a client to a server.
// net.Enable(endName, enabled) -- enable/disable a client.
// net.Reliable(bool) -- false means drop/delay messages
//
// end.Call("Raft.AppendEntries", &args, &reply) -- send an RPC, wait for reply.
// the "Raft" is the name of the server struct to be called.
// the "AppendEntries" is the name of the method to be called.
// Call() returns true to indicate that the server executed the request
// and the reply is valid.
// Call() returns false if the network lost the request or reply
// or the server is down.
// It is OK to have multiple Call()s in progress at the same time on the
// same ClientEnd.
// Concurrent calls to Call() may be delivered to the server out of order,
// since the network may re-order messages.
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return. That is, there
// is no need to implement your own timeouts around Call().
// the server RPC handler function must declare its args and reply arguments
// as pointers, so that their types exactly match the types of the arguments
// to Call().
//
// srv := MakeServer()
// srv.AddService(svc) -- a server can have multiple services, e.g. Raft and k/v
//   pass srv to net.AddServer()
//
// svc := MakeService(receiverObject) -- obj's methods will handle RPCs
//   much like Go's rpcs.Register()
//   pass svc to srv.AddService()
//

import (
	"github.com/op/go-logging"
	"reflect"
	"rpc"
)

var logger = logging.MustGetLogger("rpc/mock")

type reqMsg struct {
	endName   interface{} // name of sending ClientEnd
	svcMethod string      // e.g. "Raft.AppendEntries"
	argsType  reflect.Type
	args      []byte
	replyCh   chan replyMsg
}

type replyMsg struct {
	ok    bool
	reply []byte
}

type Peers struct {
	Ends []*ClientEnd
}

func (p *Peers) Len() int {
	return len(p.Ends)
}

func (p *Peers) GetEnd(i int) rpc.ClientEnd {
	return p.Ends[i]
}
