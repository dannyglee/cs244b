package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type HttpClient struct {
	// Used to ping registry service to get group membership.
	RegistryUrl string

	NodeId int
}

func (client *HttpClient) RequestVote(targetNodeUrl string, args *RequestVoteRequest) *RequestVoteResponse {
	postBody, _ := json.Marshal(*args)
	postBuffer := bytes.NewBuffer(postBody)
	resp, err := http.Post(fmt.Sprintf("%s/requestVote", targetNodeUrl), "application/json", postBuffer)
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

func (client *HttpClient) AppendEntries(targetNodeUrl string, args *AppendEntriesRequest) *AppendEntriesResponse {
	postBody, _ := json.Marshal(*args)
	postBuffer := bytes.NewBuffer(postBody)
	resp, err := http.Post(fmt.Sprintf("%s/appendEntries", targetNodeUrl), "application/json", postBuffer)
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
