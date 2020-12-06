package main

import (
	"net/http"
)

func AddNode(w http.ResponseWriter, req *http.Request) {

}

func StopNode(w http.ResponseWriter, req *http.Request) {

}

func StartHandler(w http.ResponseWriter, req *http.Request) {

}

func StopHandler(w http.ResponseWriter, req *http.Request) {

}

// Initially there are 4 nodes.
func init() {

}

/*
func main() {
	http.HandleFunc("/addNode", AddNode)
	http.HandleFunc("/stopNode", StopNode)
	http.HandleFunc("/startHand", StartHandler)
	http.HandleFunc("/stopHand", StopHandler)
	http.ListenAndServe(":8080", nil)

}*/
