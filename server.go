package main

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
)

type Simulator struct {
	Handler *RequestHandler
}

func (s *Simulator) StopLeaderNode(w http.ResponseWriter, req *http.Request) {
	if s.Handler.LeaderNode == -1 {
		w.WriteHeader(400)
		w.Write([]byte("No leader node yet!"))
		return
	}
	s.Handler.Nodes[s.Handler.LeaderNode].Deactivate()
	w.WriteHeader(200)
	w.Write([]byte(fmt.Sprintf("Turned off leader node: %d\n", s.Handler.LeaderNode)))
}

func (s *Simulator) StopNode(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	id := query.Get("id")
	if id == "" {
		w.WriteHeader(400)
		w.Write([]byte("Please provide node id!"))
		return
	}
	id_num, err := strconv.Atoi(id)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte("Please provide valid node id!"))
	}
	if id_num < 0 || id_num >= s.Handler.TotalNodes {
		w.WriteHeader(400)
		w.Write([]byte("Invalid node id!"))
		return
	}
	s.Handler.Nodes[id_num].Deactivate()
	w.WriteHeader(200)
	w.Write([]byte("Turned off node: " + id))
}

func (s *Simulator) StartNode(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	id := query.Get("id")
	if id == "" {
		w.WriteHeader(400)
		w.Write([]byte("Please provide node id!"))
		return
	}
	id_num, err := strconv.Atoi(id)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte("Please provide valid node id!"))
	}
	if id_num < 0 || id_num >= s.Handler.TotalNodes {
		w.WriteHeader(400)
		w.Write([]byte("Invalid node id!"))
		return
	}
	s.Handler.Nodes[id_num].Activate()
	w.WriteHeader(200)
	w.Write([]byte("Turned on node: " + id))
}

func (s *Simulator) AddPartition(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	id, ok := query["id"]
	if !ok {
		w.WriteHeader(400)
		w.Write([]byte("Please provide node id!"))
		return
	}
	id_nums := []int{}
	for _, i := range id {
		id_num, err := strconv.Atoi(i)
		if err != nil {
			w.WriteHeader(400)
			w.Write([]byte("Please provide valid node id!"))
		}
		id_nums = append(id_nums, id_num)
	}
	s.Handler.AddPartition(id_nums)
	w.WriteHeader(200)
	w.Write([]byte(fmt.Sprintf("Parition added: %d\n", s.Handler.PartitionNum)))
}

func (s *Simulator) RemovePartition(w http.ResponseWriter, req *http.Request) {
	s.Handler.RemovePartition()
	w.WriteHeader(200)
	w.Write([]byte(fmt.Sprintf("Parition removed: %d\n", s.Handler.PartitionNum+1)))
}

func (s *Simulator) ListNodes(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte(s.Handler.ListNodes()))
}

func (s *Simulator) GetEventLog(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	id := query.Get("id")
	if id == "" {
		w.WriteHeader(400)
		w.Write([]byte("Please provide node id!"))
		return
	}
	id_num, err := strconv.Atoi(id)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte("Please provide valid node id!"))
	}
	if id_num < 0 || id_num >= s.Handler.TotalNodes {
		w.WriteHeader(400)
		w.Write([]byte("Invalid node id!"))
		return
	}
	w.WriteHeader(200)
	w.Write([]byte(s.Handler.Nodes[id_num].GetEventLog()))
}

func (s *Simulator) GetCommitLog(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	id := query.Get("id")
	if id == "" {
		w.WriteHeader(400)
		w.Write([]byte("Please provide node id!"))
		return
	}
	id_num, err := strconv.Atoi(id)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte("Please provide valid node id!"))
	}
	if id_num < 0 || id_num >= s.Handler.TotalNodes {
		w.WriteHeader(400)
		w.Write([]byte("Invalid node id!"))
		return
	}
	w.WriteHeader(200)
	w.Write([]byte(s.Handler.Nodes[id_num].GetCommitLog()))
}

func (s *Simulator) StartHandler(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	n := query.Get("size")
	if n == "" {
		w.WriteHeader(400)
		w.Write([]byte("Please provide cluster size!"))
		return
	}
	num, err := strconv.Atoi(n)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte("Please provide valid cluster size!"))
	}
	s.Handler = InitHandler(num)
	go s.Handler.ClockTick()
	w.WriteHeader(200)
	w.Write([]byte("started handler"))
}

func (s *Simulator) StopHandler(w http.ResponseWriter, req *http.Request) {
	if s.Handler == nil {
		w.WriteHeader(400)
		w.Write([]byte("No handler yet!"))
	}
	s.Handler.TurnOff()
	w.WriteHeader(200)
	w.Write([]byte("Ended handler handler"))
}

func (s *Simulator) ClientSend(w http.ResponseWriter, req *http.Request) {
	if s.Handler == nil {
		w.WriteHeader(400)
		w.Write([]byte("No handler yet!"))
	}
	query := req.URL.Query()
	msg := query.Get("msg")
	fmt.Println(msg)
	res := s.Handler.PushClientMsg(msg)
	if !res {
		w.WriteHeader(400)
		w.Write([]byte("No leader yet!"))
	}
	w.WriteHeader(200)
	w.Write([]byte("Pushed the message!"))
}

func main() {
	s := &Simulator{}
	http.HandleFunc("/stopLeader", s.StopLeaderNode)
	http.HandleFunc("/startHandler", s.StartHandler)
	http.HandleFunc("/stopHandler", s.StopHandler)
	http.HandleFunc("/getCommits", s.GetCommitLog)
	http.HandleFunc("/getEvents", s.GetEventLog)
	http.HandleFunc("/stopNode", s.StopNode)
	http.HandleFunc("/startNode", s.StartNode)
	http.HandleFunc("/addPart", s.AddPartition)
	http.HandleFunc("/removePart", s.RemovePartition)
	http.HandleFunc("/listNodes", s.ListNodes)
	http.HandleFunc("/clientSend", s.ClientSend)
	log.Println("Start simulator at port 8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
