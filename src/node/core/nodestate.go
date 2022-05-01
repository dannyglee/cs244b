package core

import (
	"math"
	"sync"
)

// Type for user commands for log entry, can be updated to
// fit specific use case.
type UserCommand string

// Enum to represent node's role: {leader, follow, candidate}.
type NodeRole uint8

const (
	Leader    NodeRole = 0
	Follower           = 1
	Candidate          = 2
)

type LogEntry struct {
	termReceived int
	content      UserCommand
}

// Contains the internal state of the node.
type Node struct {
	mu sync.Mutex
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

func (node *Node) Init() {
	// TODO: Implement this.
}

// AppendEntries related struct/method.
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

func (node *Node) AppendEntries(args *AppendEntriesRequest) AppendEntriesResponse {
	node.mu.Lock()
	node.LeaderId = args.LeaderId

	// Failure cases.
	if args.Term < node.CurrentTerm || len(node.LocalLog) <= args.PrevLogIndex ||
		node.LocalLog[args.PrevLogIndex].termReceived == args.PrevLogTerm {
		node.mu.Unlock()
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
	node.mu.Unlock()
	return AppendEntriesResponse{true, node.CurrentTerm}
}

// RequestVode related struct/method.
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

func (node *Node) RequestVote(args *RequestVoteRequest) RequestVoteResponse {
	node.mu.Lock()
	if args.Term < node.CurrentTerm {
		node.mu.Unlock()
		return RequestVoteResponse{node.CurrentTerm, false}
	}
	if node.VotedFor != 0 && node.VotedFor != args.CandidateId {
		node.mu.Unlock()
		return RequestVoteResponse{node.CurrentTerm, false}
	}
	if args.LastLogIndex >= len(node.LocalLog) {
		node.mu.Unlock()
		return RequestVoteResponse{node.CurrentTerm, false}
	}

	nodeLastLogTerm := node.LocalLog[len(node.LocalLog)-1].termReceived
	if args.LastLogTerm >= nodeLastLogTerm {
		node.CurrentTerm = args.Term
		node.mu.Unlock()
		return RequestVoteResponse{node.CurrentTerm, true}
	} else {
		node.mu.Unlock()
		return RequestVoteResponse{node.CurrentTerm, false}
	}
}

// Role change function to be called by controller, where appropriate.
func (node *Node) UpdateRole(newRole NodeRole) {
	// TODO: Expand this method when we add more role change logic.
	node.Role = newRole
}

func (node *Node) AddMembers(newMembers *[]int) {
	node.mu.Lock()
	for _, nodeId := range *newMembers {
		node.ClusterMembers[nodeId] = true
	}
	node.mu.Unlock()
}

// Updates lastApplied and applies command to local state machine.
func (node *Node) applyLog() {
	node.mu.Lock()
	if node.LastApplied < node.CommitIndex {
		// TODO: Implement this when building service on top of RAFT.
	}
	node.LastApplied = node.CommitIndex
	node.mu.Unlock()
	return
}
