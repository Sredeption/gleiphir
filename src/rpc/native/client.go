package native

import "net/rpc"

type ClientEnd struct {
	endName interface{}
	address string
}

func (e *ClientEnd) Call(svcMethod string, args interface{}, reply interface{}) bool {
	c, errDial := rpc.Dial("unix", e.address)
	if errDial != nil {
		return false
	}
	defer c.Close()

	errCall := c.Call(svcMethod, args, reply)
	if errCall != nil {
		return true
	}

	return false
}
