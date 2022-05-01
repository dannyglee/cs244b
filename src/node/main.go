package main

import (
	"bufio"
	"cs244b/src/node/httpserver"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
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
			fmt.Println(fmt.Sprintf("%v", server.Controller.Node.LocalLog))
		case "showMembers":
			fmt.Println(fmt.Sprintf("%v", server.Controller.Node.ClusterMembers))
		default:
			fmt.Println("Command not supported")
		}
	}

}

func main() {
	port, _ := strconv.ParseInt(os.Args[1], 10, 64)
	nodeId, _ := strconv.ParseInt(os.Args[2], 10, 64)
	wg := new(sync.WaitGroup)
	wg.Add(2)

	server := httpserver.RaftServer{}
	server.Init(int(nodeId), int(port))

	http.HandleFunc("/appendEntries", server.AppendEntries)
	http.HandleFunc("/requestVote", server.RequestVote)
	http.HandleFunc("/add", server.ClientRequest)
	http.HandleFunc("/addMembers", server.AddMembers)
	go func() {
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
		wg.Done()
	}()

	go func() {
		userCommandHandler(server)
		wg.Done()
	}()

	wg.Wait()
}
