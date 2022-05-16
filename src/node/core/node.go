package core

import (
	"fmt"
	"math"
	"math/rand"
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
	Entries           *[]UserCommand
	LeaderCommitIndex int
}

type AppendEntriesResponse struct {
	Success    bool
	Term       int
	BadRequest bool
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
	BadRequest  bool
}

// Main APIs.
func (node *Node) AppendEntries(args *AppendEntriesRequest) AppendEntriesResponse {
	node.mu.Lock()
	defer node.mu.Unlock()
	// Early success case.
	if args.PrevLogIndex == -1 {
		node.LeaderId = args.LeaderId
		node.Role = Follower
		node.lastUpdateEpoch = time.Now().UnixMilli()
		return node.appendEntriesSuccess(args)
	}

	// Failure cases.
	if args.Term < node.CurrentTerm {
		return AppendEntriesResponse{false, node.CurrentTerm, false}
	}
	node.LeaderId = args.LeaderId
	node.Role = Follower
	node.lastUpdateEpoch = time.Now().UnixMilli()
	if len(node.LocalLog) <= args.PrevLogIndex || node.LocalLog[args.PrevLogIndex].termReceived != args.PrevLogTerm {
		return AppendEntriesResponse{false, node.CurrentTerm, false}
	}

	// Success cases.
	return node.appendEntriesSuccess(args)
}

func (node *Node) appendEntriesSuccess(args *AppendEntriesRequest) AppendEntriesResponse {
	logSize := len(node.LocalLog)
	startAppend := false
	for i, v := range *(args.Entries) {
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
	return AppendEntriesResponse{true, node.CurrentTerm, false}
}

func (node *Node) RequestVote(args *RequestVoteRequest) RequestVoteResponse {
	node.mu.Lock()
	defer node.mu.Unlock()
	if args.Term < node.CurrentTerm {
		return RequestVoteResponse{node.CurrentTerm, false, false}
	}
	if args.Term > node.CurrentTerm {
		node.Role = Follower
		node.VotedFor = -1
	}
	if node.VotedFor != -1 && node.VotedFor != args.CandidateId {
		return RequestVoteResponse{node.CurrentTerm, false, false}
	}
	if args.LastLogIndex >= len(node.LocalLog) {
		return RequestVoteResponse{node.CurrentTerm, false, false}
	}
	nodeLastLogTerm := -1
	if len(node.LocalLog) > 0 {
		nodeLastLogTerm = node.LocalLog[len(node.LocalLog)-1].termReceived
	}
	if args.LastLogTerm >= nodeLastLogTerm {
		node.lastUpdateEpoch = time.Now().UnixMilli()
		node.CurrentTerm = args.Term
		node.VotedFor = args.CandidateId
		return RequestVoteResponse{node.CurrentTerm, true, false}
	} else {
		return RequestVoteResponse{node.CurrentTerm, false, false}
	}
}

// Cluster related methods.
func (node *Node) Init(nodeId int, url string) {
	node.rpcClient = &HttpClient{RegistryUrl: "http://localhost:5000/", NodeId: nodeId}
	node.NextIndex = make(map[int]int)
	node.MatchIndex = make(map[int]int)
	node.Role = Follower
	node.CurrentTerm = 0
	node.VotedFor = -1
	node.LocalLog = []LogEntry{}
	node.lastUpdateEpoch = time.Now().UnixMilli()
	node.heartbeatIntervalMillis = 50
	node.electionTimeoutMillis = int64(math.Min(math.Max(rand.NormFloat64()*100+150, 100), 200))
	node.addToCluster(nodeId, url)
	go node.ticker()
}

func (node *Node) AddMembers(newMembers *[]int, newMemberUrls *[]string) {
	node.mu.Lock()
	defer node.mu.Unlock()
	for i, nodeId := range *newMembers {
		fmt.Println((*newMemberUrls)[i])
		node.ClusterMembers[nodeId] = true
		node.MatchIndex[nodeId] = -1
		node.NextIndex[nodeId] = len(node.LocalLog)
		node.rpcClient.NodeUrls[nodeId] = (*newMemberUrls)[i]
	}
	fmt.Println(fmt.Sprintf("Node %d received membership change", node.NodeId))
}

func (node *Node) startElection() {
	voteCount := 1
	node.CurrentTerm++
	node.LeaderId = -1
	node.VotedFor = node.NodeId
	lastLogTerm := -1
	lastLogIndex := len(node.LocalLog) - 1
	if lastLogIndex >= 0 {
		lastLogTerm = node.LocalLog[lastLogIndex].termReceived
	}
	requestVoteArgs := RequestVoteRequest{CandidateId: node.NodeId, Term: node.CurrentTerm, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	epoch := time.Now().UnixMilli()
	if voteCount > len(node.ClusterMembers)/2 {
		fmt.Println(fmt.Sprintf("Node %d elected leader", node.NodeId))
		node.Role = Leader
		node.LeaderId = node.NodeId
		node.initializeNextIndex()
		node.initializeMatchIndex()
		node.leaderSendHeartbeat()
		node.lastUpdateEpoch = epoch
		return
	}
	for nodeId := range node.ClusterMembers {
		if nodeId != node.NodeId {
			go func(id int) {
				voteResult := node.rpcClient.RequestVote(id, &requestVoteArgs)
				node.mu.Lock()
				defer node.mu.Unlock()
				if voteResult.BadRequest || node.Role == Leader {
					return
				}
				epoch := time.Now().UnixMilli()
				if node.Role != Candidate {
					return
				}
				if voteResult.VoteGranted {
					voteCount++
					if voteCount > len(node.ClusterMembers)/2 {
						fmt.Println(fmt.Sprintf("Node %d elected leader", node.NodeId))
						node.Role = Leader
						node.LeaderId = node.NodeId
						node.VotedFor = -1
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
}

// Single ticker thread to trigger state changes.
func (node *Node) ticker() {
	for {
		epoch := time.Now().UnixMilli()
		switch node.Role {
		case Leader:
			if epoch >= node.lastUpdateEpoch+node.heartbeatIntervalMillis {
				node.mu.Lock()
				node.leaderSendHeartbeat()
				node.lastUpdateEpoch = epoch
				node.mu.Unlock()
			}
		case Follower:
			if epoch >= node.lastUpdateEpoch+node.electionTimeoutMillis {
				fmt.Println("changed to candidate")
				node.Role = Candidate
			}
		case Candidate:
			if epoch >= node.lastUpdateEpoch+node.electionTimeoutMillis {
				node.startElection()
				node.lastUpdateEpoch = epoch
			}
		}
	}
}

// Leader only helper methods
func (node *Node) leaderSendHeartbeat() {
	lastIndex := len(node.LocalLog) - 1
	var termReceived int
	if lastIndex == -1 {
		termReceived = -1
	} else {
		termReceived = node.LocalLog[lastIndex].termReceived
	}
	heartbeatArgs := AppendEntriesRequest{node.CurrentTerm, node.NodeId, lastIndex,
		termReceived, &([]UserCommand{}), node.CommitIndex}
	for nodeId := range node.ClusterMembers {
		if nodeId != node.NodeId {
			go node.rpcClient.AppendEntries(nodeId, &heartbeatArgs)
		}
	}
}

func (node *Node) initializeNextIndex() {
	index := len(node.LocalLog)
	for id := range node.ClusterMembers {
		node.NextIndex[id] = index
	}
}

func (node *Node) initializeMatchIndex() {
	for id := range node.ClusterMembers {
		node.MatchIndex[id] = -1
	}
}

// Temporary group membership change protocol: wait until all nodes see new member before initialization.
func (node *Node) addToCluster(nodeId int, url string) {
	nodes := node.rpcClient.RegisterNewNode(nodeId, url)
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
		node.CommitIndex = 0
		node.LastApplied = 0
		node.CurrentTerm = 0
		fmt.Println(fmt.Sprintf("RAFT NODE INITIALIZED - nodeId: %d, serving at %s", nodeId, url))
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
	if node.LastApplied < node.CommitIndex {
		// TODO: Implement this when building service on top of RAFT.
		fmt.Println(fmt.Sprintf("Log index %d to %d applied at node %d", node.LastApplied+1, node.CommitIndex, node.NodeId))
	}
	node.LastApplied = node.CommitIndex
}

func (node *Node) HandleExternalCommand(command UserCommand) bool {
	fmt.Println("client request called")
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.Role == Leader {
		node.LocalLog = append(node.LocalLog, LogEntry{node.CurrentTerm, command})
		lastLogIndex := len(node.LocalLog) - 1
		lastLogTerm := 0
		if lastLogIndex >= 0 {
			lastLogTerm = node.LocalLog[lastLogTerm].termReceived
		} else {
			lastLogTerm = -1
		}
		successCount := 1
		for id := range node.ClusterMembers {
			if id == node.NodeId {
				continue
			}
			go func(nodeId int) {
				node.appendEntryForFollower(nodeId, &[]UserCommand{command}, &successCount)
			}(id)
		}
	}
	return true
}

func (node *Node) appendEntryForFollower(targetNodeId int, command *[]UserCommand, successCount *int) bool {
	node.mu.Lock()
	lastLogIndex := node.NextIndex[targetNodeId] - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = node.LocalLog[lastLogTerm].termReceived
	} else {
		lastLogTerm = -1
	}
	if len(node.LocalLog)-1 < lastLogIndex {
		node.mu.Unlock()
		return false
	}
	args := AppendEntriesRequest{node.CurrentTerm, node.NodeId, lastLogIndex,
		lastLogTerm, command, node.CommitIndex}
	node.mu.Unlock()
	response := node.rpcClient.AppendEntries(targetNodeId, &args)
	node.mu.Lock()
	if response.BadRequest || node.Role != Leader {
		node.mu.Unlock()
		return false
	}
	if *successCount > len(node.ClusterMembers)/2 {
		return true
	}
	if response.Success {
		*successCount++
		node.NextIndex[targetNodeId] = len(node.LocalLog)
		node.MatchIndex[targetNodeId] = len(node.LocalLog) - 1
		if *successCount > len(node.ClusterMembers)/2 {
			node.CommitIndex = len(node.LocalLog) - 1
			fmt.Printf("user command %d committed\n", node.CommitIndex)
		}
		node.mu.Unlock()
		return true
	} else {
		if response.Term > node.CurrentTerm {
			node.CurrentTerm = response.Term
			node.Role = Follower
			node.lastUpdateEpoch = time.Now().UnixMilli()
			node.mu.Unlock()
			return false
		}
		node.NextIndex[targetNodeId]--
		lastLogEntry := node.LocalLog[node.NextIndex[targetNodeId]]
		*command = append([]UserCommand{lastLogEntry.content}, *command...)
		node.mu.Unlock()
		return node.appendEntryForFollower(targetNodeId, command, successCount)
	}
}
