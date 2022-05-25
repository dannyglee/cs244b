package httpserver

import (
	"cs244b/src/node/core"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

type RaftServer struct {
	Node   *core.Node
	nodeId int
	url    string
}

func (server *RaftServer) Init(nodeId int, url string, startAsLeader bool) {
	server.nodeId = nodeId
	server.url = url
	node := core.Node{}
	node.Init(nodeId, url, startAsLeader)
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
	command := r.URL.Query().Get("command")
	commitChannel := make(chan string, 1)
	redirectChannel := make(chan string, 1)
	go server.Node.HandleExternalCommand(core.UserCommand(command), &commitChannel, &redirectChannel)
	for {
		select {
		case url := <-commitChannel:
			w.Write([]byte(fmt.Sprintf("%s", url)))
			return
		case url := <-redirectChannel:
			http.Get(fmt.Sprintf("%s/add?command=%s", url, command))
			w.Write([]byte(fmt.Sprintf("%s", url)))
			return
		}
	}
}

func (server *RaftServer) PrepareCommitGroupChange(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	input := core.PrepareCommitArgs{}
	json.Unmarshal(body, &input)
	result := server.Node.PrepareCommitGroupChange(&input)
	w.Write([]byte(fmt.Sprintf("%t", result)))
}

func (server *RaftServer) CommitGroupChange(w http.ResponseWriter, r *http.Request) {
	timeStamp, _ := strconv.ParseInt(r.URL.Query().Get("timestamp"), 10, 64)
	server.Node.CommitGroupChange(timeStamp)
}

func (server *RaftServer) AddMember(w http.ResponseWriter, r *http.Request) {
	nodeId, _ := strconv.ParseInt(r.URL.Query().Get("nodeId"), 10, 64)
	url := r.URL.Query().Get("url")
	groupMembers := make(map[int]string)
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	json.Unmarshal(body, &groupMembers)
	server.Node.AddMember(int(nodeId), url, &groupMembers)
}

func (server *RaftServer) RemoveMember(w http.ResponseWriter, r *http.Request) {
	nodeId, _ := strconv.ParseInt(r.URL.Query().Get("nodeId"), 10, 64)
	server.Node.RemoveMember(int(nodeId))
}

func (server *RaftServer) RegistryPing(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}

func (server *RaftServer) Pause(w http.ResponseWriter, r *http.Request) {
	server.Node.Pause()
}

func (server *RaftServer) Unpause(w http.ResponseWriter, r *http.Request) {
	server.Node.Unpause()
}
