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

	heartbeatIntervalMicros int64
	electionTimeoutMicros   int64
	lastUpdateEpoch         int64

	// Always non-zero, unique among cluster.
	NodeId                    int
	Role                      NodeRole
	ClusterMembers            map[int]string
	PendingMembers            map[int]string
	LeaderId                  int
	membershipChangeTimestamp int64

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

	// pause/unpause channels to simulate network delay.
	pauseChan chan bool
}

// -------------------------------------------------------------------------------------------------------------------------
// AppendEntries and helper methods
// -------------------------------------------------------------------------------------------------------------------------
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

func (node *Node) AppendEntries(args *AppendEntriesRequest) AppendEntriesResponse {
	node.mu.Lock()
	defer node.mu.Unlock()

	// Failure cases.
	if args.Term < node.CurrentTerm {
		return AppendEntriesResponse{false, node.CurrentTerm, false}
	}

	node.LeaderId = args.LeaderId
	node.changeToFollower(args.Term)
	// Early success case.
	if args.PrevLogIndex == -1 {
		resp := node.appendEntriesSuccess(args)
		return resp
	}
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
	node.LastApplied = node.CommitIndex
}

// -------------------------------------------------------------------------------------------------------------------------
// RequestVote and helper methods
// -------------------------------------------------------------------------------------------------------------------------
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
		node.lastUpdateEpoch = time.Now().UnixMicro()
		node.CurrentTerm = args.Term
		node.VotedFor = args.CandidateId
		return RequestVoteResponse{node.CurrentTerm, true, false}
	} else {
		return RequestVoteResponse{node.CurrentTerm, false, false}
	}
}

// -------------------------------------------------------------------------------------------------------------------------
// Methods that handle 2PC based cluster membership change.
// -------------------------------------------------------------------------------------------------------------------------
func (node *Node) PrepareCommitGroupChange(newMembers *map[int]string, timestamp int64) bool {
	node.mu.Lock()
	defer node.mu.Unlock()
	if timestamp > node.membershipChangeTimestamp {
		node.PendingMembers = *newMembers
		node.membershipChangeTimestamp = timestamp
		fmt.Println("pending members updated")
		return true
	}
	return false
}

func (node *Node) CommitGroupChange(timeStamp int64) {
	node.mu.Lock()
	defer node.mu.Unlock()
	if timeStamp == node.membershipChangeTimestamp {
		toAdd := make(map[int]string)
		toRemove := make(map[int]string)
		for oldNodeId, url := range node.ClusterMembers {
			if _, ok := node.PendingMembers[oldNodeId]; !ok {
				toRemove[oldNodeId] = url
			}
		}
		for newNodeId, url := range node.PendingMembers {
			if _, ok := node.ClusterMembers[newNodeId]; !ok {
				toAdd[newNodeId] = url
			}
		}
		for addNodeId, url := range toAdd {
			go node.updateNewNode(addNodeId, url)
		}
		for removeNodeId := range toRemove {
			delete(node.MatchIndex, removeNodeId)
			delete(node.NextIndex, removeNodeId)
			if removeNodeId == node.NodeId {
				// Removes itself.
				node.reset()
				return
			}
		}
		node.ClusterMembers = node.PendingMembers
		node.PendingMembers = make(map[int]string)
		node.electionTimeoutMicros = int64(math.Min(math.Max(rand.NormFloat64()*30+150, 100), 200)) * 1000
	}
}

// -------------------------------------------------------------------------------------------------------------------------
// Methods that handle single node cluster membership change.
// -------------------------------------------------------------------------------------------------------------------------
func (node *Node) AddMember(newNodeId int, url string, groupMembers *map[int]string) bool {
	node.mu.Lock()
	defer node.mu.Unlock()
	if len(*groupMembers) > 0 {
		// The [groupMembers] optional parameter contains the full updated cluster members so that
		// newly added nodes will be updated. This param is only set if newNodeId == node.NodeId.
		node.ClusterMembers = *groupMembers
		if node.Role == Leader {
			node.initializeMatchIndex()
			node.initializeNextIndex()
		}
		node.electionTimeoutMicros = int64(math.Min(math.Max(rand.NormFloat64()*30+150, 100), 200)) * 1000
	} else {
		go func() {
			node.updateNewNode(newNodeId, url)
			node.ClusterMembers[newNodeId] = url
		}()
	}
	return true
}

func (node *Node) RemoveMember(nodeId int) bool {
	node.mu.Lock()
	defer node.mu.Unlock()
	if nodeId == node.NodeId {
		// Removes itself.
		node.reset()
		return true
	}
	if _, ok := node.ClusterMembers[nodeId]; ok {
		delete(node.ClusterMembers, nodeId)
		node.electionTimeoutMicros = int64(math.Min(math.Max(rand.NormFloat64()*30+150, 100), 200)) * 1000
		if node.Role == Leader {
			delete(node.MatchIndex, nodeId)
			delete(node.NextIndex, nodeId)
		}
	}
	return true
}

func (node *Node) updateNewNode(nodeId int, url string) {
	node.perMemberLock.Lock(nodeId)
	defer node.perMemberLock.Unlock(nodeId)
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.Role == Leader {
		node.NextIndex[nodeId] = len(node.LocalLog)
		node.MatchIndex[nodeId] = -1
		commands := getCommandFromEntries(&node.LocalLog)
		args := AppendEntriesRequest{node.CurrentTerm, node.NodeId, -1,
			node.CurrentTerm, commands, node.CommitIndex}
		fmt.Printf("update node %d at %s\n", nodeId, url)
		node.mu.Unlock()
		node.rpcClient.AppendEntries(url, &args)
		node.mu.Lock()
		node.NextIndex[nodeId] = len(*commands)
		node.MatchIndex[nodeId] = len(*commands) - 1
	}
	node.electionTimeoutMicros = int64(math.Min(math.Max(rand.NormFloat64()*30+150, 100), 200)) * 1000
}

// helper method to reset node state. May be called when the current node is removed from cluster, or when the [Reset]
// endpoint is called.
func (node *Node) reset() {
	ownUrl := node.ClusterMembers[node.NodeId]
	node.ClusterMembers = make(map[int]string)
	node.ClusterMembers[node.NodeId] = ownUrl
	node.CurrentTerm = 0
	node.electionTimeoutMicros = 1000 * 1000 * 60 * 60 * 24 * 365
	node.Role = Follower
	node.LeaderId = -1
	node.VotedFor = -1
	node.LocalLog = []LogEntry{}
	node.CommitIndex = -1
	node.LastApplied = -1
}

// -------------------------------------------------------------------------------------------------------------------------
// Helper methods to simulate network partition/recovery, and node reset. Only used for our experiment.
// -------------------------------------------------------------------------------------------------------------------------
func (node *Node) Pause() {
	node.pauseChan <- true
}

func (node *Node) Unpause() {
	node.pauseChan <- false
}

// When this method is called, it's the same effect as killing/restarting the RAFT node process.
func (node *Node) Reset() {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.reset()
}

// -------------------------------------------------------------------------------------------------------------------------
// Initializes node state when they first come online. Nodes always start as followers. [standBy] parameter
// determines whether the node sets the timeout upon initialization. If set to true, it will remain in Follower state until
// it's local cluster is updated. This is to "warm" the nodes before the registry adds them to the cluster.
// -------------------------------------------------------------------------------------------------------------------------
func (node *Node) Init(nodeId int, url, registryUrl string, standBy bool) {
	node.rpcClient = &HttpClient{RegistryUrl: registryUrl, NodeId: nodeId}
	node.NextIndex = make(map[int]int)
	node.MatchIndex = make(map[int]int)
	node.pauseChan = make(chan bool)
	node.LocalLog = []LogEntry{}
	node.heartbeatIntervalMicros = 30 * 1000
	node.electionTimeoutMicros = int64(math.Min(math.Max(rand.NormFloat64()*30+150, 100), 200)) * 1000
	node.perMemberLock = NewLockMap()
	node.ClusterMembers = map[int]string{nodeId: url}
	node.PendingMembers = make(map[int]string)
	node.membershipChangeTimestamp = time.Now().UnixMicro()
	node.NodeId = nodeId
	node.CommitIndex = -1
	node.LastApplied = -1
	node.CurrentTerm = 0
	if standBy {
		// If [standBy] is set to true, we intentionally keep node as follower to prepare for
		// membership update from registry.
		node.electionTimeoutMicros = 1000 * 1000 * 60 * 60 * 24 * 365
	}
	node.changeToFollower(0)
	fmt.Println(fmt.Sprintf("RAFT NODE INITIALIZED - nodeId: %d, serving at %s", nodeId, url))
	fmt.Println("---------------------")
	go node.ticker()
	go node.pauseListener()
}

// -------------------------------------------------------------------------------------------------------------------------
// Election related methods.
// -------------------------------------------------------------------------------------------------------------------------
func (node *Node) startElection() {
	node.mu.Lock()
	defer node.mu.Unlock()
	epoch := time.Now().UnixMicro()
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
	if voteCount > len(node.ClusterMembers)/2 {
		node.changeToLeader(epoch)
		return
	}
	for nodeId := range node.ClusterMembers {
		if nodeId != node.NodeId {
			go node.handleVoteResult(node.ClusterMembers[nodeId], &requestVoteArgs, &voteCount)
		}
	}
	node.electionTimeoutMicros = int64(math.Min(math.Max(rand.NormFloat64()*30+150, 100), 200)) * 1000
}

func (node *Node) handleVoteResult(nodeUrl string, requestVoteArgs *RequestVoteRequest, voteCount *int) {
	voteResult := node.rpcClient.RequestVote(nodeUrl, requestVoteArgs)
	node.mu.Lock()
	defer node.mu.Unlock()
	if voteResult.BadRequest || node.Role == Leader {
		return
	}
	epoch := time.Now().UnixMicro()
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

// -------------------------------------------------------------------------------------------------------------------------
// Helper methods to handle role change.
// -------------------------------------------------------------------------------------------------------------------------
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
	node.lastUpdateEpoch = time.Now().UnixMicro()
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

// -------------------------------------------------------------------------------------------------------------------------
// Background threads that handle heartbeat, election timeout, and pause/unpause signals.
// -------------------------------------------------------------------------------------------------------------------------
func (node *Node) ticker() {
	for {
		epoch := time.Now().UnixMicro()
		switch node.Role {
		case Leader:
			if epoch >= node.lastUpdateEpoch+node.heartbeatIntervalMicros {
				node.mu.Lock()
				node.leaderSendHeartbeat()
				node.lastUpdateEpoch = epoch
				node.mu.Unlock()
			}
		case Follower:
			if epoch >= node.lastUpdateEpoch+node.electionTimeoutMicros {
				fmt.Println("changed to candidate")
				node.Role = Candidate
			}
		case Candidate:
			if epoch >= node.lastUpdateEpoch+node.electionTimeoutMicros {
				node.startElection()
				node.lastUpdateEpoch = epoch
			}
		}
	}
}

func (node *Node) pauseListener() {
	for {
		select {
		case pauseSig := <-node.pauseChan:
			if pauseSig {
				node.mu.Lock()
			} else {
				node.mu.Unlock()
			}
		default:
			continue
		}
	}
}

func (node *Node) leaderSendHeartbeat() {
	lastLogIndex := len(node.LocalLog) - 1
	lastLogTerm := -1
	emptyCommand := &[]UserCommand{}
	if lastLogIndex >= 0 && lastLogIndex < len(node.LocalLog) {
		lastLogTerm = node.LocalLog[lastLogIndex].termReceived
	}
	args := AppendEntriesRequest{node.CurrentTerm, node.NodeId, lastLogIndex,
		lastLogTerm, emptyCommand, node.CommitIndex}
	for nodeId := range node.ClusterMembers {
		if nodeId != node.NodeId {
			go func(id int) {
				node.rpcClient.AppendEntries(node.ClusterMembers[id], &args)
			}(nodeId)
		}
	}
}

// -------------------------------------------------------------------------------------------------------------------------
// Leader methods that processe external request to add & replicate log entry to the cluster.
// -------------------------------------------------------------------------------------------------------------------------
func (node *Node) HandleAddLogEntry(command UserCommand, commitChannel *chan string, redirectChannel *chan string) bool {
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
		node.lastUpdateEpoch = time.Now().UnixMicro()
		if len(node.ClusterMembers) == 1 {
			node.CommitIndex++
			*commitChannel <- node.ClusterMembers[node.NodeId]
			node.applyLog()
			return true
		}
		term := node.CurrentTerm
		for id := range node.ClusterMembers {
			if id == node.NodeId {
				continue
			}
			go func(nodeId int) {
				node.perMemberLock.Lock(nodeId)
				node.appendEntryForFollower(nodeId, 2, &[]LogEntry{{term, command}}, &successCount, commitChannel)
				node.perMemberLock.Unlock(nodeId)
			}(id)
		}
		return true
	} else {
		*redirectChannel <- node.ClusterMembers[node.LeaderId]
		return false
	}
}

func (node *Node) appendEntryForFollower(targetNodeId, nextIndexDecIfFail int, command *[]LogEntry, successCount *int, commitSignal *chan string) bool {
	node.mu.Lock()
	lastLogIndex := node.NextIndex[targetNodeId] - 1
	lastLogTerm := -1
	if lastLogIndex >= 0 && lastLogIndex < len(node.LocalLog) {
		lastLogTerm = node.LocalLog[lastLogIndex].termReceived
	}
	if len(node.LocalLog)-1 < lastLogIndex || node.Role != Leader {
		node.mu.Unlock()
		return false
	}
	args := AppendEntriesRequest{node.CurrentTerm, node.NodeId, lastLogIndex,
		lastLogTerm, getCommandFromEntries(command), node.CommitIndex}
	node.mu.Unlock()
	response := node.rpcClient.AppendEntries(node.ClusterMembers[targetNodeId], &args)
	node.mu.Lock()
	if response.BadRequest {
		node.mu.Unlock()
		return false
	}
	if response.Success {
		*successCount++
		node.NextIndex[targetNodeId] = lastLogIndex + len(*command) + 1
		node.MatchIndex[targetNodeId] = lastLogIndex + len(*command)
		if *successCount > len(node.ClusterMembers)/2 {
			node.CommitIndex = node.MatchIndex[targetNodeId]
			*commitSignal <- node.ClusterMembers[node.NodeId]
		}
		node.mu.Unlock()
		return true
	} else {
		if response.Term > node.CurrentTerm {
			node.changeToFollower(response.Term)
			node.mu.Unlock()
			return false
		}
		prevNextIndex := node.NextIndex[targetNodeId]
		node.NextIndex[targetNodeId] = int(math.Max(float64(node.NextIndex[targetNodeId]-nextIndexDecIfFail), float64(0)))
		unseenLogEntries := node.LocalLog[node.NextIndex[targetNodeId]:prevNextIndex]
		*command = append(unseenLogEntries, *command...)
		node.mu.Unlock()
		return node.appendEntryForFollower(targetNodeId, nextIndexDecIfFail*2, command, successCount, commitSignal)
	}
}

func getCommandFromEntries(entries *[]LogEntry) *([]UserCommand) {
	output := make([]UserCommand, len(*entries))
	for i, v := range *entries {
		output[i] = v.content
	}
	return &output
}
