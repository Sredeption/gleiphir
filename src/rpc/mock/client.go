package mock

import (
	"bytes"
	"encoding/gob"
	"reflect"
)

type ClientEnd struct {
	endName interface{} // this end-point's name
	ch      chan reqMsg // copy of Network.endCh
}

// send an RPC, wait for the reply.
// the return value indicates success; false means that
// no reply was received from the server.
func (e *ClientEnd) Call(svcMethod string, args interface{}, reply interface{}) bool {
	req := reqMsg{}
	req.endName = e.endName
	req.svcMethod = svcMethod
	req.argsType = reflect.TypeOf(args)
	req.replyCh = make(chan replyMsg)

	qb := new(bytes.Buffer)
	qe := gob.NewEncoder(qb)
	qe.Encode(args)
	req.args = qb.Bytes()

	e.ch <- req

	rep := <-req.replyCh
	if rep.ok {
		rb := bytes.NewBuffer(rep.reply)
		rd := gob.NewDecoder(rb)
		if err := rd.Decode(reply); err != nil {
			logger.Fatalf("ClientEnd.Call(): decode reply: %v\n", err)
		}
		return true
	} else {
		return false
	}
}
