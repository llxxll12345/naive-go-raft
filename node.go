package main

import (
	"strings"
	"sync"
	"time"
)

const (
	Follower = iota
	Candidate = iota
	Leader = iota
)

type Node struct {
	Id        int
	State     int 
	Term      int

	Active    bool
	Handler   *RequestHandler

	PendingMsg 	string 
	ClusterSize int
	
	ClientMsg  string	  //client message set by simulator
	CMutex	   sync.Mutex //client message lock

	CommitLog  []string
	CmtMutex   sync.Mutex
	CmtCond	   *sync.Cond

	SeqNo      int

	QMutex       sync.Mutex
	QCond        *sync.Cond
	MessageQueue []Message

	VoteReached 	bool
	LeaderSelected  bool
	EMutex      sync.Mutex
	ECond		*sync.Cond // Leader elected notification

	LeaderReached bool
	LMutex 		sync.Mutex
	LCond		*sync.Cond 
}

func InitNode(id int, h *RequestHandler) *Node {
	return &Node{
		Id:           id,
		State:        0,
		Term:         0,
		Reachable:    make(map[int]bool),
		Active:       false,
		Handler:      h,
		CommitLog:    make([]string, 0),
		MessageQueue: make([]Message, 0),
		SeqNo:        0,
		ClusterSize: h.TotalNodes,
	}
}

// Only call activate and deactivate in overall handler,
// no data race protection of n.Active, Assuming only read in nodes.
func (n *Node) Activate() {
	n.Active = true
	n.QCond = sync.NewCond(&n.QMutex)
	n.ECond = sync.NewCond(&n.EMutex)
	n.CmtCond = sync.NewCond(&n.CmtMutex)
	go n.ListenToRequest()
}

func (n *Node) Deactivate() {
	n.Active = false
}

func (n *Node) HandleRequest(m Message) {
	content := strings.Split(m.Msg, " ")
	switch content[0] {
	case "hello":
		n.ReplyInitRequest(m.Src, m.SeqNo)
	case "vote":
		n.ELock.lock()
		VoteReached = true
		n.ELock.Unlock()
		n.ECond.Broadcast()
		n.ReplyVoteRequest(m.Src, "approve", m.SeqNo)
	case "winner": 
		n.ELock.lock()
		n.LeaderSelected = true
		n.ELock.Unlock()
		n.ECond.Broadcast()
		n.ReplyWinnerRequest(m.Src, "approve", m.SeqNo)
	case "append":
		if n.Id == Leader {
			if n.Term < m.Term {
				n.StepDown(m.Term)
			} else {
				n.ReplyRequest(m.Src, "deny", m.SeqNo)
				return
			}
		}
		LeaderReached = true
		if (len(content) > 1) {
			n.PendingMsg = content[1]
		}
		n.ReplyRequest(m.Src, "ACK", m.SeqNo)
	case "commit":
		LeaderReached = true
		n.CmtMutex.Lock()
		n.CommitLog = append(n.CommitLog, n.PendingMsg)
		n.CmtMutex.Unlock()
		n.ReplyRequest(m.Src, "complete", m.SeqNo)
	}
}

// Listen to requests.
func (n *Node) ListenToRequest() {
	println("Listener start.")
	for n.Active {
		n.QMutex.Lock()
		if len(n.MessageQueue) > 0 {
			//println("Received something")
			s := n.MessageQueue[0]
			if s.Dest == n.Id && s.Code == 0 {
				n.MessageQueue = n.MessageQueue[1:]
				// Don't block the listener, spawn a new handler each time.
				go n.HandleRequest(s)
			}
		}
		n.QMutex.Unlock()
		// Wake up every 50ms
		time.Sleep(50 * time.Millisecond)
	}
}

// Push to message queue.
func (n *Node) SendAndListen(dest int, msg string) Response {
	ok := n.Handler.SimulateSend(n.Id, dest, msg, 0, n.SeqNo, n.Term)
	if !ok {
		return Response{Code: 404, Msg: "Not Found"}
	}
	n.SeqNo += 1
	n.QMutex.Lock()
	start := time.Now()

	for n.Active {
		for len(n.MessageQueue) == 0 && time.Since(start) <= 500*time.Millisecond {
			n.QCond.Wait()
		}

		// timeout after 500ms
		if len(n.MessageQueue) == 0 || time.Since(start) >= 500*time.Millisecond {
			n.QMutex.Unlock()
			return Response{Code: 408, Msg: "Timeout"}
		}

		s := n.MessageQueue[0]
		if s.Dest == n.Id && s.SeqNo == n.SeqNo {
			n.MessageQueue = n.MessageQueue[1:]
			n.QMutex.Unlock()
			return Response{Code: s.Code, Msg: s.Msg}
		}
	}
	n.QMutex.Unlock()
	return Response{Code: 404, Msg: "Not Found"}
}

func (n *Node) SendInitRequest(dest int) Response {
	return n.SendAndListen(dest, "hello")
}

func (n *Node) SendElectionResult(dest int) Response {
	return n.SendAndListen(dest, "winner " + n.Id)
}

func (n *Node) ReplyInitRequest(dest int, seqNo int) bool {
	return n.Handler.SimulateSend(n.Id, dest, "hello", 200, seqNo+1, n.Term)
}

func (n *Node) ReplyRequest(dest int, msg string, seqNo int) bool {
	return n.Handler.SimulateSend(n.Id, dest, msg, 200, seqNo+1, n.Term)
}

func (n *Node) SendVoteRequest(dest int) Response {
	return n.SendAndListen(dest, "vote")
}

// Append to log (heart beat)
func (n *Node) SendAppendRequest(dest int, Msg string) Response {
	return n.SendAndListen(dest, "append " + Msg)
}

// Commit if msg is not empty (heart beat)
func (n *Node) SendCommitRequest(dest int) Response {
	return n.SendAndListen(dest, "commit")
}

func (n *Node) StepDown(curTerm int) {
	n.Term = rsp.term
	n.State = Follower
	CommitLog = make([]String, [])
}

func (n *Node) PerformHeartBeat(dest int) {
	if n.State != Leader {
		return
	}
	cmsg := ""
	n.ClientLock.Lock()
	cmsg = n.ClientMsg
	n.ClientMsg = ""
	n.ClientLock.UnLock()

	// Add a timeout here
	cnt := 0
	futureTerm := -1
	for i := 0; i < n.ClusterSize; i++ {
		if i == n.Id {
			continue
		}
		go func(i) {
			rsp := n.SendAppendRequest(i, msg)
			if rsp.term > n.term {
				// step down
				n.StepDown(rsp.term)
				return
			} 
			if rsp.Code == 200 && rsp.Msg == "ACK" {
				n.CmtMutex.Lock()
				cnt += 1
				n.CmtMutex.Unlock()
				if cnt >= n.ClusterSize/2 {
					n.CmtCond.Broadcast()
				}
			} else if rsp.Code == 200 && rsp.Msg == "deny" {
				futureTerm = rsp.Term
			}
		}(i) 
	}
	start = time.Now()
	n.CmtMutex.Lock()
	// timeout here
	for cnt < n.ClusterSize/2 && time.Since(start) <= time.Millisecond * 500 {
		n.CmtCond.Wait()
	}

	if futureTerm != -1 {
		n.CmtMutex.Unlock()
		n.StepDown(futureTerm)
		return
	} 

	if cnt < n.ClusterSize/2 || time.Since(start) >= time.Millisecond * 500 {
		n.CmtMutex.Unlock()
		return
	}
	
	if cmsg == "" {
		n.CmtMutex.Unlock()
		return
	}
	n.CommitLog = append(n.CommitLog, n.PendingMsg)	
	n.CmtCond.Unlock()
	
	// Then wait for ack
	for i := 0; i < n.ClusterSize; i++ {
		if i == n.Id {
			continue
		}
		go func(i) {
			rsp := n.SendCommitRequest(i)
			if rsp.Code == 200 {
				fmt.Printf("Commit \"%s\" on Node %d", cmsg, i)
			}
		}(i)
	}
}

// Not receiving anything from a leader, then become a candidate.
func (n *Node) Election() {
	n.EMutex.Lock()
	start = time.Now()
	// Election time out (should be a randomized value)
	for !VoteReached && time.Since(start) <= time.Millisecond * 200 {
		n.EMutex.Wait()
	}
	if time.Since(start) > time.Millisecond * 200 {
		n.EMutex.Unlock()
		return
	}
	if VoteReached {
		for !LeaderSelected && time.Since(start) <= time.Millisecond * 200 {
			n.EMutex.Wait()
		}
		if LeaderSelected {
			n.EMutex.Unlock()
			n.State = Follower
			return
		}
	} else {
		// Add a timeout here
		n.EMutex.Unlock()
		cnt := 0
		for i := 0; i < n.ClusterSize; i++ {
			if i == n.Id {
				continue
			}
			go func(i) {
				rsp := n.SendVoteRequest(i, msg)
				n.EMutex.Lock()
				if rsp.Code == 200 && rsp.Msg == "approve" {
					cnt += 1
				} 
				n.EMutex.Unlock()
			}(i) 
		}
		start = time.Now()
		n.EMutex.Lock()
		// timeout here
		for cnt < n.ClusterSize/2 && time.Since(start) <= time.Millisecond * 500 {
			n.ECond.Wait()
		}

		if cnt >= n.ClusterSize/2 || time.Since(start) >= time.Millisecond * 500 {
			n.State = Leader
			n.Term += 1
			for i := 0; i < n.ClusterSize; i++ {
				if i == n.Id {
					continue
				}
				go func(i) {
					rsp := n.SendElectionResult(i)
				}(i) 
			}
		}
		n.EMutex.Unlock()
		if n.State == Leader {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// Leader Program, heart beat every 100 seconds
func (n *Node) Leader() {
	for n.Active && n.state == Leader {
		go n.PerformHeartBeat()
		time.Sleep(100 * time.Millisecond)
	}
}

// Follower Program
func (n *Node) Follower() {
	for n.Active && n.state == Follower {
		n.LMutex.Lock()
		start = time.Now()
		// 150 ms time out
		for !n.LeaderReached && time.Since(start) <= time.Millisecond * 150 {
			n.LCond.Wait()
		}
		if (time.Since(start) > time.Millisecond * 150) && !n.LeaderReached{
			n.LMutex.Unlock()
			n.state = Candidate
			return
		}
		n.LMutex.Unlock()
	}
}


func (n *Node) RaftRun() {
	// Election
	for n.Active {
		if n.state == Leader {
			go Leader()
		} else if n.state == Candidate {
			go Election()
		} else {
			go Follower()
		}
	}
}
