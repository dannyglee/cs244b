package main

import (
	"cs244b/src/node/httpserver"
	"cs244b/src/registry/registry_server"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
	"testing"
)

func addServerToCluster(nodeId int, url string) {
	resp, err := http.Get(fmt.Sprintf("http://localhost:5000/addSingleMember?nodeId=%d&url=%s", nodeId, url))
	if err != nil {
		log.Fatalln(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	sb := string(body)
	fmt.Printf("AddSingleMember response is %s", sb)
}

// Start a cluster of size 3.
func createCluster() []string {
	go func() {registry_server.Run(1, 10)}()
	go func() {httpserver.Run("http://localhost:4001", "4001", 1, true)}()
	go func() {httpserver.Run("http://localhost:4002", "4002", 2, false)}()
	go func() {httpserver.Run("http://localhost:4003", "4003", 3, false)}()
	time.Sleep(1 * time.Second)
	addServerToCluster(1, "http://localhost:4001")
	addServerToCluster(2, "http://localhost:4002")
	addServerToCluster(3, "http://localhost:4003")
	time.Sleep(1 * time.Second)
	return []string{"http://localhost:4001", "http://localhost:4002", "http://localhost:4003"}
}

// Given a list of servers, checks whether they agree on the leader and returns the
// leader id; -1 is returned if there are any errors.
func checkOneLeader(servers []string) int {
	leaders := make(map[int]bool)
	for _, server := range servers {
		fmt.Println(server + "/getLeader")
		resp, err := http.Get(server + "/getLeader")
		if err != nil {
			log.Fatalln(err)
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
		   log.Fatalln(err)
		}
		sb := string(body)
		leader, err := strconv.Atoi(sb)
		if err != nil {
			log.Fatalln(err)
		}
		leaders[leader] = true
	}
	if len(leaders) > 1 {
		log.Fatalf("Found more than 1 leaders: %v\n", leaders)
	} else if len(leaders) == 0 {
		log.Fatalln("Found no leader")
	} else {
		for leader := range leaders {
			fmt.Printf("Leader is %v\n", leader)
			return leader;
		}
	}
	return -1;
}

/*
func (cfg *config) checkTerms() int {
	term := -1
	for i := 0; i < cfg.n; i++ {
		if cfg.connected[i] {
			xterm, _ := cfg.rafts[i].GetState()
			if term == -1 {
				term = xterm
			} else if term != xterm {
				cfg.t.Fatalf("servers disagree on term")
			}
		}
	}
	return term
}
*/

// // Checks whether the given servers agree on the term, and returns the term if every agrees
// // on it, otherwise returns -1.
// func checkTerms(servers []string) int {

// }

const RaftElectionTimeout = 1000 * time.Millisecond

func TestInitialElection2A(t *testing.T) {
	t.Log("In TestInitialElection2A\n")
	servers := createCluster();
	time.Sleep(1 * time.Second);
	checkOneLeader(servers);

	// sleep a bit to avoid racing with followers learning of the
	// election, then check that all peers agree on the term.
	time.Sleep(50 * time.Millisecond)
	// term1 := checkTerms(servers)
	// if term1 < 1 {
	// 	t.Fatalf("term is %v, but should be at least 1", term1)
	// }

	// does the leader+term stay the same if there is no network failure?
	time.Sleep(2 * RaftElectionTimeout)
	// term2 := checkTerms(servers)
	// if term1 != term2 {
	// 	fmt.Printf("warning: term changed even though there were no failures")
	// }

	// there should still be a leader.
	checkOneLeader(servers)
}

// func main() {
// 	TestInitialElection2A()
// }