package main

import (
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
	w.Write([]byte("Turned off leader node: " + s.Handler.LeaderNode))
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
	if id_num < 0 || id_num >= s.Handler == -1 {
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
	if id_num < 0 || id_num >= s.Handler == -1 {
		w.WriteHeader(400)
		w.Write([]byte("Invalid node id!"))
		return
	}
	s.Handler.Nodes[id_num].Activate()
	w.WriteHeader(200)
	w.Write([]byte("Turned on node: " + id))
}

func (s *Simulator) AddPartition(w http.ResponseWriter, req *http.Request) {

}

func (s *Simulator) RemovePartition(w http.ResponseWriter, req *http.Request) {

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
	if id_num < 0 || id_num >= s.Handler == -1 {
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
	if id_num < 0 || id_num >= s.Handler == -1 {
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

func main() {
	http.HandleFunc("/addNode", AddNode)
	http.HandleFunc("/stopNode", StopNode)
	http.HandleFunc("/startHand", StartHandler)
	http.HandleFunc("/stopHand", StopHandler)
	http.ListenAndServe(":8080", nil)
}
