package main

import (
	"bufio"
	"cs244b/src/node/httpserver"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"
)

// This is independent to normal consensus operations, because they happen over http.
// It shows certain internal system state for debugging and demo purposes.
func userCommandHandler(server httpserver.RaftServer) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		scanner.Scan()
		userCommand := scanner.Text()
		switch userCommand {
		case "showLog":
			fmt.Println(fmt.Sprintf("%v", server.Node.LocalLog))
		case "showMembers":
			fmt.Println(fmt.Sprintf("%v", server.Node.ClusterMembers))
		case "showLeader":
			fmt.Println(fmt.Sprintf("%d", server.Node.LeaderId))
		case "showRole":
			fmt.Println(server.Node.Role)
		default:
			continue
		}
	}

}

func main() {
	fmt.Printf("Process started at %d\n", time.Now().UnixMilli())
	url := flag.String("node_url", "http://localhost", "the url this node is serving from")
	port := flag.Int("service_port", 4000, "the TCP port this node is serving from")
	nodeId := flag.Int("node_id", 0, "the current node's Id in the cluster")
	standBy := flag.Bool("stand_by", false, "whether to start the node in stand-by mode")
	registryUrl := flag.String("registry_url", "http://localhost:5000", "the url of the trusted registry")
	flag.Parse()
	wg := new(sync.WaitGroup)
	wg.Add(2)
	rand.Seed(time.Now().UnixNano())
	server := httpserver.RaftServer{}
	server.Init(*nodeId, fmt.Sprintf("%s:%d", *url, *port), *registryUrl, *standBy)

	http.HandleFunc("/appendEntries", server.AppendEntries)
	http.HandleFunc("/requestVote", server.RequestVote)
	http.HandleFunc("/add", server.AddLogEntry)
	http.HandleFunc("/prepareCommit", server.PrepareCommitGroupChange)
	http.HandleFunc("/commit", server.CommitGroupChange)
	http.HandleFunc("/ping", server.RegistryPing)
	http.HandleFunc("/addMember", server.AddMember)
	http.HandleFunc("/removeMember", server.RemoveMember)
	http.HandleFunc("/pause", server.Pause)
	http.HandleFunc("/unpause", server.Unpause)
	http.HandleFunc("/reset", server.Reset)
	go func() {
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
		wg.Done()
	}()

	go func() {
		userCommandHandler(server)
		wg.Done()
	}()

	wg.Wait()
}
