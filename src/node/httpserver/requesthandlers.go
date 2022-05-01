package httpserver

import (
	"cs244b/src/node/core"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type RaftServer struct {
	Controller *core.NodeController
	nodeId     int
	port       int
}

func (server *RaftServer) Init(nodeId, port int) {
	server.nodeId = nodeId
	server.port = port
	ctlr := core.NodeController{}
	ctlr.Init(nodeId, port)
	server.Controller = &ctlr
}

func (server *RaftServer) AppendEntries(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	input := core.AppendEntriesRequest{}
	json.Unmarshal(body, &input)
	resp := server.Controller.AppendEntries(&input)
	responseBody := fmt.Sprintf("success:%t,term:%d", resp.Success, resp.Term)
	w.Write([]byte(responseBody))
}

func (server *RaftServer) RequestVote(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	input := core.RequestVoteRequest{}
	json.Unmarshal(body, &input)
	resp := server.Controller.RequestVote(&input)
	responseBody := fmt.Sprintf("voteGranted:%t,term:%d", resp.VoteGranted, resp.Term)
	w.Write([]byte(responseBody))
}

func (server *RaftServer) ClientRequest(w http.ResponseWriter, r *http.Request) {
	command := r.FormValue("command")
	success := server.Controller.HandleExternalCommand(core.UserCommand(command))
	w.Write([]byte(fmt.Sprintf("%t", success)))
}

func (server *RaftServer) AddMembers(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	newMembers := []int{}
	json.Unmarshal(body, &newMembers)
	server.Controller.AddMembers(&newMembers)
	w.Write([]byte("DONE"))
}
