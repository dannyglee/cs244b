package core

import (
	"cs244b/src/node/httpclient"
	"fmt"
)

type NodeController struct {
	node      *Node
	rpcClient *httpclient.HttpClient
}

func (controller *NodeController) Init() {
	// TODO: Implement this.
}

func (controller *NodeController) AppendEntries(request *AppendEntriesRequest) AppendEntriesResponse {
	// TODO: Implement this.
	fmt.Println("append entries called")
	return AppendEntriesResponse{}
}

func (controller *NodeController) RequestVote(request *RequestVoteRequest) RequestVoteResponse {
	// TODO: Implement this.
	fmt.Println("request vote called")
	return RequestVoteResponse{}
}

func (controller *NodeController) HandleExternalCommand(command UserCommand) bool {
	// TODO: Implement this.
	fmt.Println("client request called")
	return true
}
