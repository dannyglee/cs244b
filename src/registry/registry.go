package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Registry struct {
	mu                          sync.Mutex
	currentMembers              map[int]string
	pendingMembers              map[int]string
	groupChangeTimestamp        int64
	healthPingEpoch             int64
	consequtiveFailedHealthPing map[int]int
	healthPingIntervalMicros    int64
	maxAllowablePingFailures    int
}

func (registry *Registry) init(healthPingIntervalSeconds, maxAllowablePingFailures int) {
	registry.currentMembers = make(map[int]string)
	registry.pendingMembers = make(map[int]string)
	registry.consequtiveFailedHealthPing = make(map[int]int)
	registry.groupChangeTimestamp = time.Now().UnixMicro()
	registry.healthPingIntervalMicros = int64(healthPingIntervalSeconds) * 1000 * 1000
	registry.healthPingEpoch = time.Now().UnixMicro()
	registry.maxAllowablePingFailures = maxAllowablePingFailures
}

// -------------------------------------------------------------------------------------------------
// Ticker function for permenant failure detection (ping process).
// -------------------------------------------------------------------------------------------------
func (registry *Registry) monitorGroupHealth() {
	for {
		epoch := time.Now().UnixMicro()
		if epoch > registry.healthPingEpoch+registry.healthPingIntervalMicros {
			registry.mu.Lock()
			for nodeId, url := range registry.currentMembers {
				if _, ok := registry.consequtiveFailedHealthPing[nodeId]; !ok {
					registry.consequtiveFailedHealthPing[nodeId] = 0
				}
				go func(nodeId int, url string) {
					pingResult := ping(url)
					registry.mu.Lock()
					if pingResult {
						registry.consequtiveFailedHealthPing[nodeId] = 0
					} else {
						registry.consequtiveFailedHealthPing[nodeId]++
						if registry.consequtiveFailedHealthPing[nodeId] >
							registry.maxAllowablePingFailures {
							go registry.addOrRemoveSingleMember(false, true, nodeId, url)
						}
					}
					registry.mu.Unlock()
				}(nodeId, url)
			}
			registry.healthPingEpoch = epoch
			registry.mu.Unlock()
		}
	}
}

// -------------------------------------------------------------------------------------------------
// Business logic for 2PC based membership change
// -------------------------------------------------------------------------------------------------
func (registry *Registry) memberChange(newMembers *map[int]string) bool {
	registry.mu.Lock()
	registry.pendingMembers = make(map[int]string)
	toAdd := make(map[int]string)
	toRemove := make(map[int]string)
	fmt.Println("member change triggered")
	for nodeId, url := range *newMembers {
		fmt.Printf("%d: %s\n", nodeId, url)
		registry.pendingMembers[nodeId] = url
		if _, ok := registry.currentMembers[nodeId]; !ok {
			toAdd[nodeId] = url
		}
	}
	for nodeId, url := range registry.currentMembers {
		if _, ok := (*newMembers)[nodeId]; !ok {
			toRemove[nodeId] = url
		}
	}
	changeTimestamp := time.Now().UnixMicro()
	registry.groupChangeTimestamp = changeTimestamp
	prepareCommitSuccessChannel := make(chan bool)
	expectedSuccessCount := len(registry.currentMembers) + len(toAdd)
	successCount := 0
	resultCount := 0
	prepareCommitArgs := PrepareCommitArgs{registry.groupChangeTimestamp, registry.pendingMembers}
	for _, url := range registry.currentMembers {
		go sendPrepareCommit(&prepareCommitArgs, url, &prepareCommitSuccessChannel)
	}
	for _, url := range toAdd {
		go sendPrepareCommit(&prepareCommitArgs, url, &prepareCommitSuccessChannel)
	}
	registry.mu.Unlock()

	voteResult := false
	for {
		select {
		case result := <-prepareCommitSuccessChannel:
			resultCount++
			if result {
				successCount++
			}
			if resultCount == expectedSuccessCount {
				voteResult = resultCount == successCount
				registry.mu.Lock()
				defer registry.mu.Unlock()
				if changeTimestamp == registry.groupChangeTimestamp && voteResult {
					registry.currentMembers = make(map[int]string)
					registry.consequtiveFailedHealthPing = make(map[int]int)
					for nodeId, url := range registry.pendingMembers {
						registry.currentMembers[nodeId] = url
						go sendCommit(url, changeTimestamp)
					}
					for _, url := range toRemove {
						go sendCommit(url, changeTimestamp)
					}
					return true
				}
				return false
			}
		}
	}
}

// -------------------------------------------------------------------------------------------------
// Business logic for singel member change. When using this method to update more than 1 members in the
// cluster, the caller must wait for the previous call to complete before adding/removing the next member
// to ensure correctness.
// -------------------------------------------------------------------------------------------------
func (registry *Registry) addOrRemoveSingleMember(isAdd, isFailureDetection bool, nodeId int, url string) bool {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	expectedSuccess := len(registry.currentMembers)
	resultCount := 0
	successCount := 0
	asyncResultChannel := make(chan bool)
	var clusterCopy map[int]string
	emptyMap := make(map[int]string)
	if isAdd == true {
		registry.currentMembers[nodeId] = url
		clusterCopy = copyCluster(&(registry.currentMembers))
		expectedSuccess++
	} else {
		if removeUrl, ok := registry.currentMembers[nodeId]; ok {
			delete(registry.currentMembers, nodeId)
			if !isFailureDetection {
				go sendRemoveMember(removeUrl, nodeId, &asyncResultChannel)
			}
		}
		if _, ok := registry.consequtiveFailedHealthPing[nodeId]; ok {
			delete(registry.consequtiveFailedHealthPing, nodeId)
		}
	}
	for id, targetUrl := range registry.currentMembers {
		if isAdd == true {
			var clusterMembers map[int]string
			if id == nodeId {
				clusterMembers = clusterCopy
			} else {
				clusterMembers = emptyMap
			}
			go sendAddMember(targetUrl, nodeId, url, &clusterMembers, &asyncResultChannel)
		} else {
			go sendRemoveMember(targetUrl, nodeId, &asyncResultChannel)
		}
	}
	for {
		select {
		case result := <-asyncResultChannel:
			resultCount++
			if result {
				successCount++
			}
			if resultCount == expectedSuccess {
				return expectedSuccess == successCount
			}
		}
	}
}

// -------------------------------------------------------------------------------------------------
// RAFT node http interface.
// -------------------------------------------------------------------------------------------------
type PrepareCommitArgs struct {
	timestamp  int64
	newMembers map[int]string
}

func sendPrepareCommit(args *PrepareCommitArgs, targetUrl string, asyncResultChannel *chan bool) {
	postBody, _ := json.Marshal(*&args.newMembers)
	fmt.Println(string(postBody))
	postBuffer := bytes.NewBuffer(postBody)
	resp, err := http.Post(fmt.Sprintf("%s/prepareCommit?timestamp=%d", targetUrl, args.timestamp), "application/json", postBuffer)
	if err != nil {
		*asyncResultChannel <- false
	}
	body, _ := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	var response bool
	json.Unmarshal(body, &response)
	*asyncResultChannel <- response
}

func sendCommit(targetUrl string, timeStamp int64) {
	http.Get(fmt.Sprintf("%s/commit?timestamp=%d", targetUrl, timeStamp))
}

func sendRemoveMember(targetUrl string, removeId int, resultChannel *chan bool) {
	_, err := http.Get(fmt.Sprintf("%s/removeMember?nodeId=%d", targetUrl, removeId))
	if err != nil {
		*resultChannel <- false
	}
	*resultChannel <- true
}

func sendAddMember(targetUrl string, nodeId int, url string, clusterMembers *map[int]string, resultChannel *chan bool) {
	postBody, _ := json.Marshal(*clusterMembers)
	postBuffer := bytes.NewBuffer(postBody)
	_, err := http.Post(fmt.Sprintf("%s/addMember?nodeId=%d&url=%s", targetUrl, nodeId, url), "application/json", postBuffer)
	if err != nil {
		*resultChannel <- false
	}
	*resultChannel <- true
}

func ping(targetUrl string) bool {
	_, err := http.Get(fmt.Sprintf("%s/ping", targetUrl))
	return err == nil
}

// -------------------------------------------------------------------------------------------------
// Request handlers for registry public APIs
// -------------------------------------------------------------------------------------------------
func (registry *Registry) HandleMembershipChange(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	input := map[int]string{}
	json.Unmarshal(body, &input)
	result := registry.memberChange(&input)
	w.Write([]byte(fmt.Sprintf("%t", result)))
}

func (registry *Registry) HandleAddSingleMember(w http.ResponseWriter, r *http.Request) {
	nodeId, _ := strconv.ParseInt(r.URL.Query().Get("nodeId"), 10, 0)
	url := r.URL.Query().Get("url")
	result := registry.addOrRemoveSingleMember(true, false, int(nodeId), url)
	w.Write([]byte(fmt.Sprintf("%t", result)))
}

func (registry *Registry) HandleRemoveSingleMember(w http.ResponseWriter, r *http.Request) {
	nodeId, _ := strconv.ParseInt(r.URL.Query().Get("nodeId"), 10, 0)
	result := registry.addOrRemoveSingleMember(false, false, int(nodeId), "")
	w.Write([]byte(fmt.Sprintf("%t", result)))
}

// -------------------------------------------------------------------------------------------------
// Helper mthods.
// -------------------------------------------------------------------------------------------------
func copyCluster(src *map[int]string) map[int]string {
	dst := make(map[int]string)
	for k, v := range *src {
		dst[k] = v
	}
	return dst
}
