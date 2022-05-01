package core

import (
	"cs244b/src/node/httpclient"
	"fmt"
)

// This is the core of the system. This controller should handle role specific functionalities
// (e.g. calling appendEntries as a leader to followers, calling requestVote as a candidate,
// process role transitions).
type NodeController struct {
	Node      *Node
	rpcClient *httpclient.HttpClient
}

func (controller *NodeController) Init(nodeId, port int) {
	controller.rpcClient = &httpclient.HttpClient{RegistryUrl: "http://localhost:5000/", NodeId: nodeId}
	controller.addToCluster(nodeId, port)
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

func (controller *NodeController) AddMembers(newMembers *[]int) {
	fmt.Println("new members added")
	controller.Node.AddMembers(newMembers)
}

func (controller *NodeController) addToCluster(nodeId, port int) {
	nodes := controller.rpcClient.RegisterNewNode(nodeId, port)
	confirmationChannel := make(chan bool)
	confirmationCountTarget := len(nodes)
	for nodeId := range nodes {
		go controller.rpcClient.AddNewMember(nodeId, confirmationChannel)
	}
	success := waitForCount(confirmationChannel, confirmationCountTarget)
	if success {
		nodes[nodeId] = true
		// TODO: Implement this.
		controller.Node = &Node{ClusterMembers: nodes, NodeId: nodeId}
		controller.Node.Init()
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
