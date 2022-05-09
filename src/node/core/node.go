package core

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// Type for user commands for log entry, can be updated to
// fit specific use case.
type UserCommand string

// Enum to represent node's role: {leader, follow, candidate}.
type NodeRole string

const (
	Leader    NodeRole = "Leader"
	Follower           = "Follower"
	Candidate          = "Candidate"
)

type LogEntry struct {
	termReceived int
	content      UserCommand
}

// Contains the internal state of the node and rpcClient to talk to other nodes.
type Node struct {
	mu sync.Mutex

	rpcClient *HttpClient

	heartbeatIntervalMillis int64
	electionTimeoutMillis   int64
	lastUpdateEpoch         int64

	// Always non-zero, unique among cluster.
	NodeId         int
	Role           NodeRole
	ClusterMembers map[int]bool
	LeaderId       int

	// Common state on all nodes.
	CommitIndex int
	LastApplied int

	// Always persisted on disk. (can be improved)
	CurrentTerm int
	VotedFor    int
	LocalLog    []LogEntry

	// Leader specific states.
	NextIndex  map[int]int
	MatchIndex map[int]int
}

// Request/response data structures.
type AppendEntriesRequest struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []UserCommand
	LeaderCommitIndex int
}

type AppendEntriesResponse struct {
	Success bool
	Term    int
}

type RequestVoteRequest struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

// Main APIs.
func (node *Node) AppendEntries(args *AppendEntriesRequest) AppendEntriesResponse {
	node.mu.Lock()
	defer node.mu.Unlock()
	// Failure cases.
	if args.Term < node.CurrentTerm {
		return AppendEntriesResponse{false, node.CurrentTerm}
	}

	node.LeaderId = args.LeaderId
	node.Role = Follower
	node.lastUpdateEpoch = time.Now().UnixMilli()

	if len(node.LocalLog) <= args.PrevLogIndex || node.LocalLog[args.PrevLogIndex].termReceived == args.PrevLogTerm {
		return AppendEntriesResponse{false, node.CurrentTerm}
	}

	// Success cases.
	logSize := len(node.LocalLog)
	startAppend := false
	for i, v := range args.Entries {
		startAppend = startAppend || args.PrevLogIndex+i+1 >= logSize
		if startAppend {
			node.LocalLog = append(node.LocalLog, LogEntry{args.Term, v})
			continue
		}
		node.LocalLog[args.PrevLogIndex+i+1] = LogEntry{args.Term, v}
	}
	newCommitIndex := math.Max(float64(node.CommitIndex), math.Min(float64(args.LeaderCommitIndex), float64(len(node.LocalLog)-1)))
	node.CommitIndex = int(newCommitIndex)
	node.applyLog()
	node.CurrentTerm = args.Term
	return AppendEntriesResponse{true, node.CurrentTerm}
}

func (node *Node) RequestVote(args *RequestVoteRequest) RequestVoteResponse {
	node.mu.Lock()
	defer node.mu.Unlock()
	if args.Term < node.CurrentTerm {
		return RequestVoteResponse{node.CurrentTerm, false}
	}
	if args.Term > node.CurrentTerm {
		node.Role = Follower
		node.VotedFor = -1
	}
	if node.VotedFor != -1 && node.VotedFor != args.CandidateId {
		return RequestVoteResponse{node.CurrentTerm, false}
	}
	if args.LastLogIndex >= len(node.LocalLog) {
		return RequestVoteResponse{node.CurrentTerm, false}
	}

	nodeLastLogTerm := node.LocalLog[len(node.LocalLog)-1].termReceived
	if args.LastLogTerm >= nodeLastLogTerm {
		node.lastUpdateEpoch = time.Now().UnixMilli()
		node.CurrentTerm = args.Term
		node.VotedFor = args.CandidateId
		return RequestVoteResponse{node.CurrentTerm, true}
	} else {
		return RequestVoteResponse{node.CurrentTerm, false}
	}
}

// Cluster related methods.
func (node *Node) Init(nodeId, port int) {
	node.rpcClient = &HttpClient{RegistryUrl: "http://localhost:5000/", NodeId: nodeId}
	node.NextIndex = make(map[int]int)
	node.MatchIndex = make(map[int]int)
	node.CurrentTerm = 0
	node.VotedFor = -1
	node.LocalLog = []LogEntry{}
	node.lastUpdateEpoch = time.Now().UnixMilli()
	node.addToCluster(nodeId, port)
}

func (node *Node) AddMembers(newMembers *[]int) {
	node.mu.Lock()
	defer node.mu.Unlock()
	for _, nodeId := range *newMembers {
		node.ClusterMembers[nodeId] = true
	}
	fmt.Printf("Node %d received membership change", node.NodeId)
}

func (node *Node) startElection() {
	voteCount := 1
	node.CurrentTerm++
	node.LeaderId = -1
	node.VotedFor = node.NodeId
	requestVoteArgs := RequestVoteRequest{}
	for nodeId := range node.ClusterMembers {
		if nodeId == node.NodeId {
			continue
		}
		go func(id int) {
			voteResult := node.rpcClient.RequestVote(id, &requestVoteArgs)
			node.mu.Lock()
			defer node.mu.Unlock()
			epoch := time.Now().UnixMilli()
			if node.Role != Candidate {
				return
			}
			if voteResult.VoteGranted {
				voteCount++
				if voteCount > len(node.ClusterMembers)/2 {
					node.Role = Leader
					node.initializeNextIndex()
					node.initializeMatchIndex()
					node.leaderSendHeartbeat()
					node.lastUpdateEpoch = epoch
				}
			} else if voteResult.Term > node.CurrentTerm {
				node.CurrentTerm = voteResult.Term
				node.Role = Follower
				node.VotedFor = -1
				node.lastUpdateEpoch = epoch
			}
		}(nodeId)
	}
}

// Single ticker thread to trigger state changes.
func (node *Node) ticker() {
	for {
		node.mu.Lock()
		epoch := time.Now().UnixMilli()
		switch node.Role {
		case Leader:
			if epoch >= node.lastUpdateEpoch+node.heartbeatIntervalMillis {
				node.leaderSendHeartbeat()
				node.lastUpdateEpoch = epoch
			}
		case Follower:
			if epoch >= node.lastUpdateEpoch+node.electionTimeoutMillis {
				node.Role = Candidate
				node.startElection()
				node.lastUpdateEpoch = epoch
			}
		case Candidate:
			if epoch >= node.lastUpdateEpoch+node.electionTimeoutMillis {
				node.Role = Candidate
				node.startElection()
				node.lastUpdateEpoch = epoch
			}
		}
		node.mu.Unlock()
	}
}

// Leader only helper methods
func (node *Node) leaderSendHeartbeat() {
	node.mu.Lock()
	defer node.mu.Unlock()
	lastIndex := len(node.LocalLog) - 1
	lastEntry := node.LocalLog[lastIndex]
	heartbeatArgs := AppendEntriesRequest{node.CurrentTerm, node.NodeId, lastIndex,
		lastEntry.termReceived, []UserCommand{}, node.CommitIndex}
	for nodeId := range node.ClusterMembers {
		go node.rpcClient.AppendEntries(nodeId, &heartbeatArgs)
	}
}

func (node *Node) initializeNextIndex() {
	index := len(node.LocalLog)
	for id := range node.ClusterMembers {
		if id == node.NodeId {
			continue
		}
		node.NextIndex[id] = index
	}
}

func (node *Node) initializeMatchIndex() {
	for id := range node.ClusterMembers {
		if id == node.NodeId {
			continue
		}
		node.MatchIndex[id] = 0
	}
}

// Temporary group membership change protocol: wait until all nodes see new member before initialization.
func (node *Node) addToCluster(nodeId, port int) {
	nodes := node.rpcClient.RegisterNewNode(nodeId, port)
	confirmationChannel := make(chan bool)
	confirmationCountTarget := len(nodes)
	for id := range nodes {
		go node.rpcClient.AddNewMember(id, confirmationChannel)
	}
	success := waitForCount(confirmationChannel, confirmationCountTarget)
	if success {
		nodes[nodeId] = true
		// TODO: Implement this.
		node.ClusterMembers = nodes
		node.NodeId = nodeId
		node.Role = Follower
		node.CommitIndex = 0
		node.LastApplied = 0
		node.CurrentTerm = 0
		fmt.Println(fmt.Sprintf("RAFT NODE INITIALIZED - nodeId: %d, serving at: localhost:%d", nodeId, port))
		fmt.Println("---------------------")
	} else {
		fmt.Println(fmt.Sprintf("Failed to initialize node %d because cluster membership update failed", nodeId))
	}
}

func waitForCount(asyncChannel chan bool, targetCount int) bool {
	totalCount := 0
	successCount := 0
	if targetCount == 0 {
		return true
	}
	for i := range asyncChannel {
		if i {
			successCount++
		}
		totalCount++
		if totalCount >= targetCount {
			break
		}
	}
	return totalCount == successCount
}

// Client related methods.

// Updates lastApplied and applies command to local state machine.
func (node *Node) applyLog() {
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.LastApplied < node.CommitIndex {
		// TODO: Implement this when building service on top of RAFT.
		fmt.Printf("Log index %d to %d applied at node %d", node.LastApplied+1, node.CommitIndex, node.NodeId)
	}
	node.LastApplied = node.CommitIndex
}

func (node *Node) HandleExternalCommand(command UserCommand) bool {
	// TODO: Implement this.
	fmt.Println("client request called")
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.Role == Leader {
		node.LocalLog = append(node.LocalLog, LogEntry{node.CurrentTerm, command})
		lastLogIndex := len(node.LocalLog) - 1
		lastLogTerm := 0
		if lastLogIndex > 0 {
			lastLogTerm = node.LocalLog[lastLogTerm].termReceived
		} else {
			lastLogTerm = -1
		}
		args := AppendEntriesRequest{node.CurrentTerm, node.NodeId, lastLogIndex,
			lastLogTerm, []UserCommand{command}, node.CommitIndex}
		successCount := 1
		for id := range node.ClusterMembers {
			if id == node.NodeId {
				continue
			}
			go func(nodeId int) {
				response := node.rpcClient.AppendEntries(nodeId, &args)
				node.mu.Lock()
				defer node.mu.Unlock()
				if response.Success {
					successCount++
					node.NextIndex[nodeId] = lastLogIndex + 2
					node.MatchIndex[nodeId] = lastLogIndex + 1
					if successCount > len(node.ClusterMembers)/2 {
						node.CommitIndex = lastLogIndex + 1
					}
				}
			}(id)
		}
	}
	return true
}
