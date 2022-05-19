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

	perMemberLock *LockMap

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
	VotedFor	int
	LocalLog    []LogEntry

	// Leader specific states.
	NextIndex  map[int]int
	MatchIndex map[int]int

	votedFor    map[int]int
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
		node.changeToFollower(args.Term)
		resp := node.appendEntriesSuccess(args)
		return resp
	}

	// Failure cases.
	if args.Term < node.CurrentTerm {
		return AppendEntriesResponse{false, node.CurrentTerm, false}
	}
	node.LeaderId = args.LeaderId
	node.changeToFollower(args.Term)
	if len(node.LocalLog) <= args.PrevLogIndex || node.LocalLog[args.PrevLogIndex].termReceived != args.PrevLogTerm {
		return AppendEntriesResponse{false, node.CurrentTerm, false}
	}

	// Success cases.
	resp := node.appendEntriesSuccess(args)
	return resp
}

func (node *Node) appendEntriesSuccess(args *AppendEntriesRequest) AppendEntriesResponse {
	logSize := len(node.LocalLog)
	startAppend := false
	for i, v := range *(args.Entries) {
		startAppend = startAppend || args.PrevLogIndex+i+1 >= logSize
		if startAppend {
			node.LocalLog = append(node.LocalLog, LogEntry{args.Term, v})
		} else {
			node.LocalLog[args.PrevLogIndex+i+1] = LogEntry{args.Term, v}
		}
	}
	newCommitIndex := math.Max(float64(node.CommitIndex), math.Min(float64(args.LeaderCommitIndex), float64(len(node.LocalLog)-1)))
	node.CommitIndex = int(newCommitIndex)
	node.applyLog()
	return AppendEntriesResponse{true, node.CurrentTerm, false}
}

func (node *Node) applyLog() {
	if node.LastApplied < node.CommitIndex {
		fmt.Println(fmt.Sprintf("Log index %d to %d applied at node %d, values are %v",
			node.LastApplied+1, node.CommitIndex, node.NodeId, node.LocalLog[node.LastApplied+1:node.CommitIndex+1]))
	}
	node.LastApplied = node.CommitIndex
}

// TODO: Migrate RequestVote from MIT lab. CALLEE
func (node *Node) RequestVote(args *RequestVoteRequest) RequestVoteResponse {
	node.mu.Lock()
	defer node.mu.Unlock()
	if args.Term < node.CurrentTerm {
		return RequestVoteResponse{node.CurrentTerm, false, false}
	}
	if args.Term > node.CurrentTerm {
		node.changeToFollower(args.Term)
	}
	if node.VotedFor != -1 && node.VotedFor != args.CandidateId {
		return RequestVoteResponse{node.CurrentTerm, false, false}
	}
	if args.LastLogIndex < len(node.LocalLog)-1 {
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
	node.LocalLog = []LogEntry{}
	node.heartbeatIntervalMillis = 50
	node.electionTimeoutMillis = int64(math.Min(math.Max(rand.NormFloat64()*10+150, 100), 200))
	node.perMemberLock = NewLockMap()
	node.addToCluster(nodeId, url)
	node.changeToFollower(0)
	go node.ticker()
}

func (node *Node) AddMembers(newMembers *[]int, newMemberUrls *[]string) {
	node.mu.Lock()
	defer node.mu.Unlock()
	for i, nodeId := range *newMembers {
		node.ClusterMembers[nodeId] = true
		node.MatchIndex[nodeId] = -1
		node.NextIndex[nodeId] = len(node.LocalLog)
		node.rpcClient.NodeUrls[nodeId] = (*newMemberUrls)[i]
		fmt.Println(fmt.Sprintf("Node %d added to cluster", nodeId))
	}
}

// Not thread-safe
func (node *Node) getLastLogIndexAndTerm() (int, int) {
	var lastLogIdx int;
	var lastLogTerm int;
	if len(node.LocalLog) == 0 {
		lastLogIdx = -1
		lastLogTerm = -1
	} else {
		lastLogIdx = len(node.LocalLog) - 1
		lastLogTerm = node.LocalLog[lastLogIdx].termReceived
	}
	return lastLogIdx, lastLogTerm
}

/*
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
		node.changeToLeader(epoch)
		return
	}
	for nodeId := range node.ClusterMembers {
		if nodeId != node.NodeId {
			go node.handleVoteResult(nodeId, &requestVoteArgs, &voteCount)
		}
	}
}
*/


// TODO: Migrate RequestVote from MIT lab. CALLER
func (node *Node) startElection() {
	node.mu.Lock()
	node.CurrentTerm++
	node.LeaderId = -1
	node.votedFor[node.CurrentTerm] = node.NodeId
	startTime := time.Now()

	term := node.CurrentTerm
	me := node.NodeId
	lastLogIdx, lastLogTerm := node.getLastLogIndexAndTerm()
	
	var roundLock sync.Mutex
	voteCount := 1
	yesVoteCount := 1  // Always vote for myself.
	for i := 0; i < len(node.ClusterMembers); i++ {
		if i == node.NodeId {
			continue
		}
		go func (dest int) {
			for {			
				voteResponse := node.rpcClient.sendRequestVote(dest, RequestVoteRequest{term, me, lastLogIdx, lastLogTerm});
				if !voteResponse.BadRequest {
					break
				}
				time.Sleep(kRpcInterval)
			}
			if voteResponse.Term > node.CurrentTerm {
				node.changeToFollower(voteResponse.Term)
			}
			roundLock.Lock()
			voteCount++
			if vote.VoteGranted {
				yesVoteCount++
			}
			roundLock.Unlock()
		} (i)
	}
	// Waiting for the election result
	for {
		roundLock.Lock()
		if yesVoteCount >= len(node.peers) / 2 + 1  || voteCount == len(node.peers) || time.Now().Sub(startTime) > kElectionRoundTimeout {
			roundLock.Unlock()
			break
		}
		roundLock.Unlock()
		time.Sleep(time.Millisecond)
	}
	electionSucceeds := yesVoteCount >= len(node.peers) / 2 + 1
	node.mu.Unlock()
	if electionSucceeds {
		node.becomeNewLeader()
	}
}

func (node *Node) handleVoteResult(nodeId int, requestVoteArgs *RequestVoteRequest, voteCount *int) {
	voteResult := node.rpcClient.RequestVote(nodeId, requestVoteArgs)
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
		*voteCount++
		if *voteCount > len(node.ClusterMembers)/2 {
			node.changeToLeader(epoch)
			return
		}
	} else if voteResult.Term > node.CurrentTerm {
		node.changeToFollower(voteResult.Term)
	}
}

func (node *Node) changeToLeader(epoch int64) {
	fmt.Println(fmt.Sprintf("Node %d elected leader", node.NodeId))
	node.Role = Leader
	node.LeaderId = node.NodeId
	node.VotedFor = -1
	node.initializeNextIndex()
	node.initializeMatchIndex()
	node.leaderSendHeartbeat()
	node.lastUpdateEpoch = epoch
}

func (node *Node) changeToFollower(newTerm int) {
	node.CurrentTerm = newTerm
	node.Role = Follower
	node.VotedFor = -1
	node.votedFor[newTerm] = -1
	node.lastUpdateEpoch = time.Now().UnixMilli()
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
	successCount := 1
	for nodeId := range node.ClusterMembers {
		if nodeId != node.NodeId {
			go func(id int) {
				node.perMemberLock.Lock(id)
				node.appendEntryForFollower(id, &[]UserCommand{}, &successCount, true)
				node.perMemberLock.Unlock(id)
			}(nodeId)
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
	node.mu.Lock()
	defer node.mu.Unlock()
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
		node.CommitIndex = -1
		node.LastApplied = -1
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
func (node *Node) HandleExternalCommand(command UserCommand) int {
	fmt.Println("client request called")
	if node.Role == Leader {
		node.mu.Lock()
		node.LocalLog = append(node.LocalLog, LogEntry{node.CurrentTerm, command})
		lastLogIndex := len(node.LocalLog) - 1
		lastLogTerm := 0
		if lastLogIndex >= 0 {
			lastLogTerm = node.LocalLog[lastLogTerm].termReceived
		} else {
			lastLogTerm = -1
		}
		successCount := 1
		node.lastUpdateEpoch = time.Now().UnixMilli()
		if len(node.ClusterMembers) == 1 {
			node.CommitIndex++
			return node.NodeId
		}
		for id := range node.ClusterMembers {
			if id == node.NodeId {
				continue
			}
			go func(nodeId int) {
				node.perMemberLock.Lock(nodeId)
				node.appendEntryForFollower(nodeId, &[]UserCommand{command}, &successCount, false)
				node.perMemberLock.Unlock(nodeId)
			}(id)
		}
		node.mu.Unlock()
		return node.NodeId
	}
	return node.LeaderId
}

func (node *Node) appendEntryForFollower(targetNodeId int, command *[]UserCommand, successCount *int, isHeartbeat bool) bool {
	node.mu.Lock()
	lastLogIndex := node.NextIndex[targetNodeId] - 1
	lastLogTerm := -1
	if lastLogIndex >= 0 && lastLogIndex < len(node.LocalLog) {
		lastLogTerm = node.LocalLog[lastLogIndex].termReceived
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
	if response.Success {
		*successCount++
		node.NextIndex[targetNodeId] = lastLogIndex + len(*command) + 1
		node.MatchIndex[targetNodeId] = lastLogIndex + len(*command)
		if *successCount > len(node.ClusterMembers)/2 {
			node.CommitIndex = node.MatchIndex[targetNodeId]
		}
		node.mu.Unlock()
		return true
	} else {
		if response.Term > node.CurrentTerm {
			node.changeToFollower(response.Term)
			node.mu.Unlock()
			return false
		}
		node.NextIndex[targetNodeId]--
		lastLogEntry := node.LocalLog[node.NextIndex[targetNodeId]]
		*command = append([]UserCommand{lastLogEntry.content}, *command...)
		node.mu.Unlock()
		return node.appendEntryForFollower(targetNodeId, command, successCount, isHeartbeat)
	}
}
