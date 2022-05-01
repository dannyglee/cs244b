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
	nodeId         int
	role           NodeRole
	clusterMembers map[int]bool
	leaderId       int

	// Common state on all nodes.
	commitIndex int
	lastApplied int

	// Always persisted on disk. (can be improved)
	currentTerm int
	votedFor    int
	localLog    []LogEntry

	// Leader specific states.
	nextIndex  map[int]int
	matchIndex map[int]int
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
	node.leaderId = args.LeaderId

	// Failure cases.
	if args.Term < node.currentTerm || len(node.localLog) <= args.PrevLogIndex ||
		node.localLog[args.PrevLogIndex].termReceived == args.PrevLogTerm {
		node.mu.Unlock()
		return AppendEntriesResponse{false, node.currentTerm}
	}

	// Success cases.
	logSize := len(node.localLog)
	startAppend := false
	for i, v := range args.Entries {
		startAppend = startAppend || args.PrevLogIndex+i+1 >= logSize
		if startAppend {
			node.localLog = append(node.localLog, LogEntry{args.Term, v})
			continue
		}
		node.localLog[args.PrevLogIndex+i+1] = LogEntry{args.Term, v}
	}
	newCommitIndex := math.Max(float64(node.commitIndex), math.Min(float64(args.LeaderCommitIndex), float64(len(node.localLog)-1)))
	node.commitIndex = int(newCommitIndex)
	node.applyLog()
	node.currentTerm = args.Term
	node.mu.Unlock()
	return AppendEntriesResponse{true, node.currentTerm}
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
	if args.Term < node.currentTerm {
		node.mu.Unlock()
		return RequestVoteResponse{node.currentTerm, false}
	}
	if node.votedFor != 0 && node.votedFor != args.CandidateId {
		node.mu.Unlock()
		return RequestVoteResponse{node.currentTerm, false}
	}
	if args.LastLogIndex >= len(node.localLog) {
		node.mu.Unlock()
		return RequestVoteResponse{node.currentTerm, false}
	}

	nodeLastLogTerm := node.localLog[len(node.localLog)-1].termReceived
	if args.LastLogTerm >= nodeLastLogTerm {
		node.currentTerm = args.Term
		node.mu.Unlock()
		return RequestVoteResponse{node.currentTerm, true}
	} else {
		node.mu.Unlock()
		return RequestVoteResponse{node.currentTerm, false}
	}
}

// Role change function to be called by controller, where appropriate.
func (node *Node) UpdateRole(newRole NodeRole) {
	// TODO: Expand this method when we add more role change logic.
	node.role = newRole
}

// Updates lastApplied and applies command to local state machine.
func (node *Node) applyLog() {
	if node.lastApplied < node.commitIndex {
		// TODO: Implement this when building service on top of RAFT.
	}
	node.lastApplied = node.commitIndex
	return
}
