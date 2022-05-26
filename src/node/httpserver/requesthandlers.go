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

func (server *RaftServer) Init(nodeId int, url, registryUrl string, standBy bool) {
	server.nodeId = nodeId
	server.url = url
	node := core.Node{}
	node.Init(nodeId, url, registryUrl, standBy)
	server.Node = &node
}

// Main endpoint for AppendEntries RPC. Should only be called by RAFT nodes.
func (server *RaftServer) AppendEntries(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	input := core.AppendEntriesRequest{}
	json.Unmarshal(body, &input)
	resp := server.Node.AppendEntries(&input)
	responseBody, _ := json.Marshal(resp)
	w.Write([]byte(responseBody))
}

// Main endpoint for RequestVote RPC. Should only be called by RAFT nodes.
func (server *RaftServer) RequestVote(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	input := core.RequestVoteRequest{}
	json.Unmarshal(body, &input)
	resp := server.Node.RequestVote(&input)
	responseBody, _ := json.Marshal(resp)
	w.Write([]byte(responseBody))
}

// Endpoint exposed to client, adds entry to the distributed log in the cluster, returns when the new entry is committed.
func (server *RaftServer) AddLogEntry(w http.ResponseWriter, r *http.Request) {
	command := r.URL.Query().Get("command")
	commitChannel := make(chan string, 1)
	redirectChannel := make(chan string, 1)
	go server.Node.HandleAddLogEntry(core.UserCommand(command), &commitChannel, &redirectChannel)
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

// Endpoint exposed to the registry. Responds to the "prepare" message of the 2PC group membership change
// initiated by the registry.
func (server *RaftServer) PrepareCommitGroupChange(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	timestamp, _ := strconv.ParseInt(r.URL.Query().Get("timestamp"), 10, 64)
	body, _ := io.ReadAll(r.Body)
	input := make(map[int]string)
	json.Unmarshal(body, &input)
	result := server.Node.PrepareCommitGroupChange(&input, timestamp)
	w.Write([]byte(fmt.Sprintf("%t", result)))
}

// Similar to [PrepareCommitGroupChange], this endpoint responds to the "commit" message of the 2PC membership change.
func (server *RaftServer) CommitGroupChange(w http.ResponseWriter, r *http.Request) {
	timeStamp, _ := strconv.ParseInt(r.URL.Query().Get("timestamp"), 10, 64)
	server.Node.CommitGroupChange(timeStamp)
}

// Endpoint exposed to the registry. Adds a single node to the cluster.
func (server *RaftServer) AddMember(w http.ResponseWriter, r *http.Request) {
	nodeId, _ := strconv.ParseInt(r.URL.Query().Get("nodeId"), 10, 64)
	url := r.URL.Query().Get("url")
	groupMembers := make(map[int]string)
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	json.Unmarshal(body, &groupMembers)
	server.Node.AddMember(int(nodeId), url, &groupMembers)
}

// Endpoint exposed to the registry. Removes a single node to the cluster.
func (server *RaftServer) RemoveMember(w http.ResponseWriter, r *http.Request) {
	nodeId, _ := strconv.ParseInt(r.URL.Query().Get("nodeId"), 10, 64)
	server.Node.RemoveMember(int(nodeId))
}

// Simple endpoint to respond to registry ping to register liveliness, this doesn't require
// locking on the node's internal state.
func (server *RaftServer) RegistryPing(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}

// Helper method to simulate network partition.
func (server *RaftServer) Pause(w http.ResponseWriter, r *http.Request) {
	server.Node.Pause()
}

// Helper method to simulate recovery from network partition.
func (server *RaftServer) Unpause(w http.ResponseWriter, r *http.Request) {
	server.Node.Unpause()
}

func (server *RaftServer) Reset(w http.ResponseWriter, r *http.Request) {
	server.Node.Reset()
	w.Write([]byte("ok"))
}
