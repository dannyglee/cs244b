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
	resp, _ := http.Post(fmt.Sprintf("%s/requestVote", client.NodeUrls[targetNodeId]), "application/json", postBuffer)
	body, _ := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	responseData := RequestVoteResponse{}
	json.Unmarshal(body, &responseData)
	return &responseData
}

func (client *HttpClient) AppendEntries(targetNodeId int, args *AppendEntriesRequest) *AppendEntriesResponse {
	postBody, _ := json.Marshal(*args)
	postBuffer := bytes.NewBuffer(postBody)
	resp, _ := http.Post(fmt.Sprintf("%s/appendEntries", client.NodeUrls[targetNodeId]), "application/json", postBuffer)
	body, _ := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	responseData := AppendEntriesResponse{}
	json.Unmarshal(body, &responseData)
	return &responseData
}

func (client *HttpClient) AddNewMember(targetNodeId int, ackChannel chan bool) {
	postBody, _ := json.Marshal([]int{client.NodeId})
	postBuffer := bytes.NewBuffer(postBody)
	_, err := http.Post(fmt.Sprintf("%s/addMembers", client.NodeUrls[targetNodeId]), "application/json", postBuffer)
	if err != nil {
		ackChannel <- false
		return
	}
	ackChannel <- true
}

func (client *HttpClient) RegisterNewNode(nodeId, port int) map[int]bool {
	resp, _ := http.Get(fmt.Sprintf("%s/register?nodeId=%d&port=%d", client.RegistryUrl, nodeId, port))
	body, _ := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	json.Unmarshal(body, &client.NodeUrls)
	keys := map[int]bool{}
	for k := range client.NodeUrls {
		keys[k] = true
	}
	return keys
}
