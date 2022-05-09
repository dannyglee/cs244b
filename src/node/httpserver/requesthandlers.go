package httpserver

import (
	"cs244b/src/node/core"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type RaftServer struct {
	Node   *core.Node
	nodeId int
	port   int
}

func (server *RaftServer) Init(nodeId, port int) {
	server.nodeId = nodeId
	server.port = port
	node := core.Node{}
	node.Init(nodeId, port)
	server.Node = &node
}

func (server *RaftServer) AppendEntries(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	input := core.AppendEntriesRequest{}
	json.Unmarshal(body, &input)
	resp := server.Node.AppendEntries(&input)
	responseBody, _ := json.Marshal(resp)
	w.Write([]byte(responseBody))
}

func (server *RaftServer) RequestVote(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	input := core.RequestVoteRequest{}
	json.Unmarshal(body, &input)
	resp := server.Node.RequestVote(&input)
	responseBody, _ := json.Marshal(resp)
	w.Write([]byte(responseBody))
}

func (server *RaftServer) ClientRequest(w http.ResponseWriter, r *http.Request) {
	command := r.FormValue("command")
	success := server.Node.HandleExternalCommand(core.UserCommand(command))
	w.Write([]byte(fmt.Sprintf("%t", success)))
}

func (server *RaftServer) AddMembers(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	newMembers := []int{}
	json.Unmarshal(body, &newMembers)
	server.Node.AddMembers(&newMembers)
	w.Write([]byte("DONE"))
}
