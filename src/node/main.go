package main

import (
	"bufio"
	"cs244b/src/node/httpserver"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
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
	url := os.Args[1]
	port := os.Args[2]
	nodeId, _ := strconv.Atoi(os.Args[3])
	wg := new(sync.WaitGroup)
	wg.Add(2)
	rand.Seed(time.Now().UnixNano())
	server := httpserver.RaftServer{}
	server.Init(int(nodeId), url, true)

	http.HandleFunc("/appendEntries", server.AppendEntries)
	http.HandleFunc("/requestVote", server.RequestVote)
	http.HandleFunc("/add", server.ClientRequest)
	http.HandleFunc("/addMembers", server.AddMembers)
	go func() {
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
		wg.Done()
	}()

	go func() {
		userCommandHandler(server)
		wg.Done()
	}()

	wg.Wait()
}
