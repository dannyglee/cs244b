package httpserver

import (
	"cs244b/src/node/core"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
)

type RaftServer struct {
	controller *core.NodeController
}

func (server *RaftServer) Init() {
	ctlr := core.NodeController{}
	ctlr.Init()
	server.controller = &ctlr
}

func (server *RaftServer) AppendEntries(w http.ResponseWriter, r *http.Request) {
	term, _ := strconv.ParseInt(r.FormValue("term"), 10, 64)
	leaderId, _ := strconv.ParseInt(r.FormValue("leaderId"), 10, 64)
	prevLogIndex, _ := strconv.ParseInt(r.FormValue("prevLogIndex"), 10, 64)
	prevLogTerm, _ := strconv.ParseInt(r.FormValue("prevLogTerm"), 10, 64)
	entries := parseEntries(r.FormValue("entries"))
	leaderCommitIndex, _ := strconv.ParseInt(r.FormValue("leaderCommitIndex"), 10, 64)
	input := core.AppendEntriesRequest{int(term), int(leaderId), int(prevLogIndex), int(prevLogTerm), entries, int(leaderCommitIndex)}
	resp := server.controller.AppendEntries(&input)
	responseBody := fmt.Sprintf("success:%t,term:%d", resp.Success, resp.Term)
	w.Write([]byte(responseBody))
}

func (server *RaftServer) RequestVote(w http.ResponseWriter, r *http.Request) {
	term, _ := strconv.ParseInt(r.FormValue("term"), 10, 64)
	candidateId, _ := strconv.ParseInt(r.FormValue("candidateId"), 10, 64)
	LastLogIndex, _ := strconv.ParseInt(r.FormValue("LastLogIndex"), 10, 64)
	LastLogTerm, _ := strconv.ParseInt(r.FormValue("LastLogTerm"), 10, 64)
	input := core.RequestVoteRequest{int(term), int(candidateId), int(LastLogIndex), int(LastLogTerm)}
	resp := server.controller.RequestVote(&input)
	responseBody := fmt.Sprintf("voteGranted:%t,term:%d", resp.VoteGranted, resp.Term)
	w.Write([]byte(responseBody))
}

func (server *RaftServer) ClientRequest(w http.ResponseWriter, r *http.Request) {
	command := r.FormValue("command")
	success := server.controller.HandleExternalCommand(core.UserCommand(command))
	w.Write([]byte(fmt.Sprintf("%t", success)))
}

func parseEntries(rawValue string) []core.UserCommand {
	re := regexp.MustCompile(",")
	split := re.Split(rawValue, -1)
	output := make([]core.UserCommand, len(split))
	return output
}
