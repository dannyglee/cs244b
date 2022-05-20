package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Registry struct {
	mu                   sync.Mutex
	currentMembers       map[int]string
	pendingMembers       map[int]string
	groupChangeTimestamp int64
}

func (registry *Registry) init() {
	registry.currentMembers = make(map[int]string)
	registry.pendingMembers = make(map[int]string)
	registry.groupChangeTimestamp = time.Now().UnixNano()
}

// -------------------------------------------------------------------------------------------------
// Business logic for 2PC membership change
// -------------------------------------------------------------------------------------------------
func (registry *Registry) memberChange(newMembers *map[int]string) bool {
	registry.mu.Lock()
	registry.pendingMembers = make(map[int]string)
	toAdd := make(map[int]string)
	for nodeId, url := range *newMembers {
		registry.pendingMembers[nodeId] = url
		if _, ok := registry.currentMembers[nodeId]; !ok {
			toAdd[nodeId] = url
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
					for nodeId, url := range registry.pendingMembers {
						registry.currentMembers[nodeId] = url
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
// Business logic for singel member change.
// -------------------------------------------------------------------------------------------------
func (registry *Registry) addOrRemoveSingleMember(isAdd bool, nodeId int, url string) bool {
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
			go sendRemoveMember(removeUrl, nodeId, &asyncResultChannel)
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
// Http client methods to call RAFT endpoints
// -------------------------------------------------------------------------------------------------
type PrepareCommitArgs struct {
	timestamp  int64
	newMembers map[int]string
}

func sendPrepareCommit(args *PrepareCommitArgs, targetUrl string, asyncResultChannel *chan bool) {
	postBody, _ := json.Marshal(*args)
	postBuffer := bytes.NewBuffer(postBody)
	resp, err := http.Post(fmt.Sprintf("%s/prepareCommit", targetUrl), "application/json", postBuffer)
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
	result := registry.addOrRemoveSingleMember(true, int(nodeId), url)
	w.Write([]byte(fmt.Sprintf("%t", result)))
}

func (registry *Registry) HandleRemoveSingleMember(w http.ResponseWriter, r *http.Request) {
	nodeId, _ := strconv.ParseInt(r.URL.Query().Get("nodeId"), 10, 0)
	result := registry.addOrRemoveSingleMember(false, int(nodeId), "")
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

func main() {

	fmt.Printf("Process started at %d\n", time.Now().UnixMilli())
	wg := new(sync.WaitGroup)
	wg.Add(1)

	// Initialize registry.
	registry := Registry{}
	registry.init()

	http.HandleFunc("/addSingleMember", registry.HandleAddSingleMember)
	http.HandleFunc("/removeSingleMember", registry.HandleRemoveSingleMember)
	http.HandleFunc("/updateGroup", registry.HandleMembershipChange)

	go func() {
		log.Fatal(http.ListenAndServe(":5000", nil))
		wg.Done()
	}()

	wg.Wait()
}
