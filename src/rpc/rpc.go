package rpc

type Service interface {
}

type Server interface {
	AddService(service *Service)
}

type Network interface {
	AddServer(serverName interface{}, server *Server)
	MakeEnd(endName interface{}) *ClientEnd
	GetEnds() map[interface{}]*ClientEnd
}

type ClientEnd interface {
	Call(svcMethod string, args interface{}, reply interface{}) bool
}

type Peers interface {
	Len() int
	GetEnd(i int) ClientEnd
}
