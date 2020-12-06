package main

import (
	"time"
)

type RequestHandler struct {
	Nodes       []*Node
	ActiveNodes int
	TotalNodes  int
	Active      bool
}

type Message struct {
	SeqNo int // sequence number
	Src   int
	Dest  int
	Msg   string
	Code  int
	Term  int
}

type Response struct {
	Msg  string
	Code int
}

// Read or pop from the queue
func InitHandler(n int) *RequestHandler {
	r := &RequestHandler{}
	r.Nodes = make([]*Node, n)
	for i := 0; i < n; i++ {
		r.Nodes[i] = InitNode(i, r)
	}
	r.TotalNodes = n
	r.ActiveNodes = 0
	r.Active = true
	return r
}

// Write to the message queue of a certain node
func (r *RequestHandler) SimulateSend(src, dest int, msg string, code int, seqNo int, term int) bool {
	//time.Sleep(time.Second)
	if src < 0 || dest < 0 || src >= len(r.Nodes) || dest >= len(r.Nodes) {
		return false
	}
	if r.Nodes[src].Active == false || r.Nodes[dest].Active == false {
		return false
	}
	r.Nodes[dest].QMutex.Lock()
	m := Message{Src: src, Dest: dest, Msg: msg, Code: code, SeqNo: seqNo, Term: term}
	r.Nodes[dest].MessageQueue = append(r.Nodes[dest].MessageQueue, m)
	r.Nodes[dest].QMutex.Unlock()
	r.Nodes[dest].QCond.Broadcast()
	return true
}

// Signal all the nodes to prevent always waiting.
func (r *RequestHandler) ClockTick() {
	for r.Active {
		println("Tick")
		for _, n := range r.Nodes {
			if n.Active {
				n.QCond.Broadcast()
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func main() {
	r := InitHandler(10)
	r.Active = true
	r.ActiveNodes = 2
	r.Nodes[0].Activate()
	r.Nodes[1].Activate()
	go r.ClockTick()
	rsp := r.Nodes[0].SendInitRequest(1)
	println(rsp.Code, rsp.Msg)
	time.Sleep(2 * time.Second)
	r.Active = false
}
