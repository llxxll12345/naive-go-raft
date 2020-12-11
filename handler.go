package main

import (
	"fmt"
	"time"
)

type RequestHandler struct {
	Nodes        []*Node
	LeaderNode   int
	TotalNodes   int
	Active       bool
	PartitionNum int
	Partition    []int // Partition of each node
}

type Message struct {
	SeqNo int // sequence number
	Src   int
	Dest  int
	Msg   string
	Code  int
	Term  int
}

// Read or pop from the queue
func InitHandler(n int) *RequestHandler {
	r := &RequestHandler{}

	r.TotalNodes = n
	r.LeaderNode = -1
	r.Active = true
	r.PartitionNum = 0
	r.Partition = make([]int, n)

	r.Nodes = make([]*Node, n)
	for i := 0; i < n; i++ {
		r.Nodes[i] = InitNode(i, r)
		r.Nodes[i].Activate()
	}
	go r.ClockTick()
	return r
}

func (r *RequestHandler) AddPartition(ids []int) {
	r.PartitionNum += 1
	for i, _ := range ids {
		if i < 0 || i >= r.TotalNodes {
			continue
		}
		r.Partition[i] = r.PartitionNum
	}
}

func (r *RequestHandler) RemovePartition() {
	if r.PartitionNum == 0 {
		return
	}
	for i := 0; i < r.TotalNodes; i++ {
		if r.Partition[i] == r.PartitionNum {
			r.Partition[i] -= 1
		}
	}
	r.PartitionNum -= 1
}

func (r *RequestHandler) TurnOff() {
	for i := 0; i < r.TotalNodes; i++ {
		r.Nodes[i].Deactivate()
	}
	r.Active = false
	r.LeaderNode = -1
}

func ParseState(state int) string {
	switch state {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	}
	return ""
}

func (r *RequestHandler) ListNodes() string {
	reply := "Nodes in cluster:\n"
	for i := 0; i < r.TotalNodes; i++ {
		n := r.Nodes[i]
		if n.Active {
			reply += fmt.Sprintf("Id: %d, state: %s, Up for: %s\n", n.Id, ParseState(n.State), time.Since(n.StartTime))
		}
	}
	return reply
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
	println(msg)
	m := Message{Src: src, Dest: dest, Msg: msg, Code: code, SeqNo: seqNo, Term: term}

	if code == 0 { // request message
		r.Nodes[dest].RequestMutex.Lock()
		r.Nodes[dest].RequestQueue = append(r.Nodes[dest].RequestQueue, m)
		r.Nodes[dest].RequestMutex.Unlock()
	} else { // reply message
		println("REPLY")
		r.Nodes[dest].ReplyMutex.Lock()
		r.Nodes[dest].ReplyMap[m.SeqNo] = m
		r.Nodes[dest].ReplyMutex.Unlock()
		r.Nodes[dest].ReplyCond.Broadcast()
	}

	return true
}

// Push a client message to the leader node
func (r *RequestHandler) PushClientMsg(msg string) bool {
	if r.LeaderNode == -1 {
		return false
	}
	r.Nodes[r.LeaderNode].AppendClientMsg(msg)
	return true
}

// Signal all the nodes to prevent always waiting.
func (r *RequestHandler) ClockTick() {
	cnt := 0
	for r.Active {
		if cnt == 10 {
			cnt = 0
			println("Tick")
		}
		cnt += 1
		for _, n := range r.Nodes {
			// Wake up to do timeout check.
			if n.Active {
				n.ReplyCond.Broadcast()
				n.ECond.Broadcast()
				n.LCond.Broadcast()
				n.CmtCond.Broadcast()
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}
