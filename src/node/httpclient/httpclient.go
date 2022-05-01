package httpclient

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
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	json.Unmarshal(body, &client.NodeUrls)
	keys := map[int]bool{}
	for k := range client.NodeUrls {
		keys[k] = true
	}
	return keys
}
