package mock

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"strings"
	"sync"
)

//
// a server is a collection of services, all sharing
// the same rpc dispatcher. so that e.g. both a Raft
// and a k/v server can listen to the same rpc endpoint.
//
type Server struct {
	mu       sync.Mutex
	services map[string]*Service
	count    int // incoming RPCs
}

func MakeServer() *Server {
	rs := &Server{}
	rs.services = map[string]*Service{}
	return rs
}

func (rs *Server) AddService(service *Service) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.services[service.name] = service
}

func (rs *Server) dispatch(req reqMsg) replyMsg {
	rs.mu.Lock()

	rs.count += 1

	// split Raft.AppendEntries into service and method
	dot := strings.LastIndex(req.svcMethod, ".")
	serviceName := req.svcMethod[:dot]
	methodName := req.svcMethod[dot+1:]

	service, ok := rs.services[serviceName]

	rs.mu.Unlock()

	if ok {
		return service.dispatch(methodName, req)
	} else {
		var choices []string
		for k := range rs.services {
			choices = append(choices, k)
		}
		logger.Fatalf("mock.Server.dispatch(): unknown service %v in %v.%v; expecting one of %v\n",
			serviceName, serviceName, methodName, choices)
		return replyMsg{false, nil}
	}
}

func (rs *Server) GetCount() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.count
}

// an object with methods that can be called via RPC.
// a single server may have more than one Service.
type Service struct {
	name     string
	receiver reflect.Value
	typ      reflect.Type
	methods  map[string]reflect.Method
}

func MakeService(receiver interface{}) *Service {
	svc := &Service{}
	svc.typ = reflect.TypeOf(receiver)
	svc.receiver = reflect.ValueOf(receiver)
	svc.name = reflect.Indirect(svc.receiver).Type().Name()
	svc.methods = map[string]reflect.Method{}

	for m := 0; m < svc.typ.NumMethod(); m++ {
		method := svc.typ.Method(m)
		mType := method.Type
		mName := method.Name

		//fmt.Printf("%v pp %v ni %v 1k %v 2k %v no %v\n",
		//	mName, method.PkgPath, mType.NumIn(), mType.In(1).Kind(), mType.In(2).Kind(), mType.NumOut())

		if method.PkgPath != "" || // capitalized?
			mType.NumIn() != 3 ||
			//mType.In(1).Kind() != reflect.Ptr ||
			mType.In(2).Kind() != reflect.Ptr ||
			mType.NumOut() != 1 {
			// the method is not suitable for a handler
			logger.Debugf("bad method: %v\n", mName)
		} else {
			// the method looks like a handler
			svc.methods[mName] = method
		}
	}

	return svc
}

func (svc *Service) dispatch(methodName string, req reqMsg) replyMsg {
	if method, ok := svc.methods[methodName]; ok {
		// prepare space into which to read the argument.
		// the Value's type will be a pointer to req.argsType.
		args := reflect.New(req.argsType)

		// decode the argument.
		ab := bytes.NewBuffer(req.args)
		ad := gob.NewDecoder(ab)
		ad.Decode(args.Interface())

		// allocate space for the reply.
		replyType := method.Type.In(2)
		replyType = replyType.Elem()
		replyV := reflect.New(replyType)

		// call the method.
		function := method.Func
		function.Call([]reflect.Value{svc.receiver, args.Elem(), replyV})

		// encode the reply.
		rb := new(bytes.Buffer)
		re := gob.NewEncoder(rb)
		re.EncodeValue(replyV)

		return replyMsg{true, rb.Bytes()}
	} else {
		var choices []string
		for k := range svc.methods {
			choices = append(choices, k)
		}
		logger.Fatalf("mock.Service.dispatch(): unknown method %v in %v; expecting one of %v\n",
			methodName, req.svcMethod, choices)
		return replyMsg{false, nil}
	}
}
