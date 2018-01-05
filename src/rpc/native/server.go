package native

import (
	"net"
	"net/rpc"
)

type Service struct {
	receiver interface{}
}

func MakeService(receiver interface{}) *Service {
	service := new(Service)
	service.receiver = receiver
	return service
}

type Server struct {
	address  string
	rpcs     *rpc.Server
	listener net.Listener
	shutdown chan struct{}
}

func MakeServer(address string) *Server {
	server := new(Server)
	server.address = address
	server.rpcs = rpc.NewServer()
	server.shutdown = make(chan struct{})
	return server
}

func (s *Server) AddService(service *Service) {
	s.rpcs.Register(service.receiver)
}

func (s *Server) start() {
	l, e := net.Listen("unix", s.address)
	if e != nil {
		logger.Criticalf("RegistrationServer", s.address, " error: ", e)
	}
	s.listener = l

loop:
	for {
		select {
		case <-s.shutdown:
			break loop
		default:
		}
		conn, err := s.listener.Accept()
		if err == nil {
			go func() {
				s.rpcs.ServeConn(conn)
				conn.Close()
			}()
		} else {
			logger.Criticalf("RegistrationServer: accept error", err)
			break
		}
	}
	logger.Infof("RegistrationServer: done\n")
}

func (s *Server) stop() error {
	logger.Infof("Shutdown: registration server\n")
	close(s.shutdown)
	s.listener.Close() // causes the Accept to fail
	return nil
}
