package native

import "net/rpc"

type ClientEnd struct {
	endName interface{}
	address string
}

func (e *ClientEnd) Call(svcMethod string, args interface{}, reply interface{}) bool {
	c, dialErr := rpc.Dial("tcp", e.address)
	if dialErr != nil {
		logger.Debug(dialErr)
		return false
	}
	defer c.Close()

	callErr := c.Call(svcMethod, args, reply)
	if callErr != nil {
		logger.Debug(callErr)
		return false
	}

	return true
}
