package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type HttpClient struct {
	// NodeId to serving url mapping for all active nodes in cluster.
	NodeUrls map[int]string

	// Used to ping registry service to get group membership.
	RegistryUrl string

	NodeId int
}

func (client *HttpClient) RequestVote(targetNodeId int, args *RequestVoteRequest) *RequestVoteResponse {
	postBody, _ := json.Marshal(*args)
	postBuffer := bytes.NewBuffer(postBody)
	resp, err := http.Post(fmt.Sprintf("%s/requestVote", client.NodeUrls[targetNodeId]), "application/json", postBuffer)
	if err != nil {
		return &RequestVoteResponse{BadRequest: true}
	}
	body, _ := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	responseData := RequestVoteResponse{}
	json.Unmarshal(body, &responseData)
	responseData.BadRequest = false
	return &responseData
}

func (client *HttpClient) AppendEntries(targetNodeId int, args *AppendEntriesRequest) *AppendEntriesResponse {
	postBody, _ := json.Marshal(*args)
	postBuffer := bytes.NewBuffer(postBody)
	resp, err := http.Post(fmt.Sprintf("%s/appendEntries", client.NodeUrls[targetNodeId]), "application/json", postBuffer)
	if err != nil {
		return &AppendEntriesResponse{BadRequest: true}
	}
	body, _ := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	responseData := AppendEntriesResponse{}
	responseData.BadRequest = false
	json.Unmarshal(body, &responseData)
	return &responseData
}

func (client *HttpClient) AddNewMember(targetNodeId int, ackChannel chan bool) {
	_, err := http.Get(fmt.Sprintf("%s/addMembers?nodeId=%d&url=%s",
		client.NodeUrls[targetNodeId], client.NodeId, client.NodeUrls[client.NodeId]))
	if err != nil {
		ackChannel <- false
		return
	}
	ackChannel <- true
}

func (client *HttpClient) RegisterNewNode(nodeId int, nodeUrl string) map[int]bool {
	resp, err := http.Get(fmt.Sprintf("%s/register?nodeId=%d&url=%s", client.RegistryUrl, nodeId, nodeUrl))
	if err != nil {
		return make(map[int]bool)
	}
	body, _ := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	json.Unmarshal(body, &client.NodeUrls)
	keys := map[int]bool{}
	for k := range client.NodeUrls {
		keys[k] = true
	}
	client.NodeUrls[nodeId] = nodeUrl
	return keys
}

func (client *HttpClient) ForwardClientCall(command UserCommand, leaderId int) bool {
	_, err := http.Get(fmt.Sprintf("%s/add?command=%d", client.NodeUrls[leaderId], command))
	if err != nil {
		return false
	}
	return true
}
