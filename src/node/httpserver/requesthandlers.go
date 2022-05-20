package httpserver

import (
	"cs244b/src/node/core"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

type RaftServer struct {
	Node                *core.Node
	nodeId              int
	url                 string
	useSimulatedLatency bool
}

func (server *RaftServer) Init(nodeId int, url string, useSimulatedLatency, startAsLeader bool) {
	server.nodeId = nodeId
	server.url = url
	node := core.Node{}
	node.Init(nodeId, url, startAsLeader)
	server.Node = &node
	server.useSimulatedLatency = useSimulatedLatency
}

func (server *RaftServer) AppendEntries(w http.ResponseWriter, r *http.Request) {
	if server.useSimulatedLatency {
		blockRequest()
	}
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	input := core.AppendEntriesRequest{}
	json.Unmarshal(body, &input)
	resp := server.Node.AppendEntries(&input)
	responseBody, _ := json.Marshal(resp)
	w.Write([]byte(responseBody))
}

func (server *RaftServer) RequestVote(w http.ResponseWriter, r *http.Request) {
	if server.useSimulatedLatency {
		blockRequest()
	}
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
	leaderId := server.Node.HandleExternalCommand(core.UserCommand(command))
	w.Write([]byte(fmt.Sprintf("%d", leaderId)))
}

func (server *RaftServer) PrepareCommitGroupChange(w http.ResponseWriter, r *http.Request) {
	if server.useSimulatedLatency {
		blockRequest()
	}
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	input := core.PrepareCommitArgs{}
	json.Unmarshal(body, &input)
	result := server.Node.PrepareCommitGroupChange(&input)
	w.Write([]byte(fmt.Sprintf("%t", result)))
}

func (server *RaftServer) CommitGroupChange(w http.ResponseWriter, r *http.Request) {
	if server.useSimulatedLatency {
		blockRequest()
	}
	timeStamp, _ := strconv.ParseInt(r.URL.Query().Get("timestamp"), 10, 64)
	server.Node.CommitGroupChange(timeStamp)
}

func (server *RaftServer) AddOrRemoveMember(w http.ResponseWriter, r *http.Request) {
	if server.useSimulatedLatency {
		blockRequest()
	}
	defer r.Body.Close()
	removeId, _ := strconv.ParseInt(r.URL.Query().Get("removeId"), 10, 64)
	body, _ := io.ReadAll(r.Body)
	input := make(map[int]string)
	json.Unmarshal(body, &input)
	server.Node.AddOrRemoveMember(&input, int(removeId))
}

func (server *RaftServer) RegistryPing(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}

func blockRequest() {
	// Assume normal latency dist. with 1.5ms mean and std. dev. of 1ms.
	latencyMicros := math.Max(rand.NormFloat64()+1.5, 1) * 1000
	time.Sleep(time.Duration(latencyMicros) * time.Microsecond)
}
