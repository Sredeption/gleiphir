package native

import (
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

type JunkArgs struct {
	X int
}
type JunkReply struct {
	X string
}

type JunkServer struct {
	mu   sync.Mutex
	log1 []string
	log2 []int
}

func (js *JunkServer) Handler1(args string, reply *int) error {
	js.mu.Lock()
	defer js.mu.Unlock()
	js.log1 = append(js.log1, args)
	*reply, _ = strconv.Atoi(args)
	return nil
}

func (js *JunkServer) Handler2(args int, reply *string) error {
	js.mu.Lock()
	defer js.mu.Unlock()
	js.log2 = append(js.log2, args)
	*reply = "handler2-" + strconv.Itoa(args)
	return nil
}

func (js *JunkServer) Handler3(args int, reply *int) error {
	js.mu.Lock()
	defer js.mu.Unlock()
	time.Sleep(20 * time.Second)
	*reply = -args
	return nil
}

// args is a pointer
func (js *JunkServer) Handler4(args *JunkArgs, reply *JunkReply) error {
	reply.X = "pointer"
	return nil
}

// args is a not pointer
func (js *JunkServer) Handler5(args JunkArgs, reply *JunkReply) error {
	reply.X = "no pointer"
	return nil
}

func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(4)

	network := MakeNetwork()

	e := network.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer("localhost:7888")
	rs.AddService(svc)
	network.AddServer("server99", rs)
	go rs.Start()

	time.Sleep(10 * time.Millisecond)

	network.Connect("end1-99", "server99")

	{
		reply := ""
		e.Call("JunkServer.Handler2", 111, &reply)
		if reply != "handler2-111" {
			t.Fatalf("wrong reply from Handler2")
		}
	}

	{
		reply := 0
		e.Call("JunkServer.Handler1", "9099", &reply)
		if reply != 9099 {
			t.Fatalf("wrong reply from Handler1")
		}
	}

	rs.Stop()

}
