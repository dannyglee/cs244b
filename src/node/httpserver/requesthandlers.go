package httpserver

import (
	"cs244b/src/node/core"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
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

func (server *RaftServer) AddMember(w http.ResponseWriter, r *http.Request) {
	if server.useSimulatedLatency {
		blockRequest()
	}
	nodeId, _ := strconv.ParseInt(r.URL.Query().Get("nodeId"), 10, 64)
	url := r.URL.Query().Get("url")
	groupMembers := make(map[int]string)
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	json.Unmarshal(body, &groupMembers)
	server.Node.AddMember(int(nodeId), url, &groupMembers)
}

func (server *RaftServer) RemoveMember(w http.ResponseWriter, r *http.Request) {
	if server.useSimulatedLatency {
		blockRequest()
	}
	nodeId, _ := strconv.ParseInt(r.URL.Query().Get("nodeId"), 10, 64)
	server.Node.RemoveMember(int(nodeId))
}

func (server *RaftServer) RegistryPing(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}

func (server *RaftServer) GetLeader(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(fmt.Sprintf("%d", server.Node.GetLeader())))
}

func (server *RaftServer) GetMembership(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(fmt.Sprintf("%v", server.Node.GetMembership())))
}

func blockRequest() {
	// Assume normal latency dist. with 1.5ms mean and std. dev. of 1ms.
	latencyMicros := math.Max(rand.NormFloat64()+1.5, 1) * 1000
	time.Sleep(time.Duration(latencyMicros) * time.Microsecond)
}

// TODO(jin): move this to a separate file in this package.
func Run(url string, port string, nodeId int, startAsLeader bool) {
	fmt.Printf("Process started at %d\n", time.Now().UnixMilli())
	wg := new(sync.WaitGroup)
	wg.Add(1)
	rand.Seed(time.Now().UnixNano())
	server := RaftServer{}
	server.Init(int(nodeId), url, true, startAsLeader)

	mux := http.NewServeMux()
	mux.HandleFunc("/appendEntries", server.AppendEntries)
	mux.HandleFunc("/requestVote", server.RequestVote)
	mux.HandleFunc("/add", server.ClientRequest)
	mux.HandleFunc("/prepareCommit", server.PrepareCommitGroupChange)
	mux.HandleFunc("/commit", server.CommitGroupChange)
	mux.HandleFunc("/ping", server.RegistryPing)
	mux.HandleFunc("/addMember", server.AddMember)
	mux.HandleFunc("/removeMember", server.RemoveMember)
	mux.HandleFunc("/getLeader", server.GetLeader)
	mux.HandleFunc("/getMembership", server.GetMembership)

	http_server := http.Server {
		Addr: ":" + port,
		Handler: mux,
	}
	go func() {
		log.Fatal(http_server.ListenAndServe())
		wg.Done()
	}()
	wg.Wait()
}
