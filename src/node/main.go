package main

import (
	"cs244b/src/node/httpserver"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
)

func UserCommandHandler() {
	fmt.Println("RAFT NODE INITIALIZED")
	fmt.Println("---------------------")

}

func main() {
	port := os.Args[1]
	nodeId := os.Args[2]
	wg := new(sync.WaitGroup)
	wg.Add(2)

	server := httpserver.RaftServer{}
	server.Init()

	http.Get(fmt.Sprintf("http://localhost:5000/register?nodeId=%s&port=%s", nodeId, port))

	http.HandleFunc("/appendEntries", server.AppendEntries)
	http.HandleFunc("/requestVote", server.RequestVote)
	http.HandleFunc("/add", server.ClientRequest)
	go func() {
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
		wg.Done()
	}()

	go func() {
		fmt.Println("RAFT NODE INITIALIZED")
		fmt.Println("---------------------")
		wg.Done()
	}()

	wg.Wait()
}
