package main

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
)

func main() {
	wg := new(sync.WaitGroup)
	wg.Add(2)

	// NodeId to port mapping.
	activeNodes := map[int]int{}

	http.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		nodeId, _ := strconv.ParseInt(r.FormValue("nodeId"), 10, 64)
		portNumber, _ := strconv.ParseInt(r.FormValue("port"), 10, 64)
		activeNodes[int(nodeId)] = int(portNumber)
		fmt.Println(fmt.Sprintf("Node %d started serving at port %d", nodeId, portNumber))
		w.Write([]byte(fmt.Sprintf("%v", activeNodes)))
	})

	go func() {
		log.Fatal(http.ListenAndServe(":5000", nil))
		wg.Done()
	}()

	wg.Wait()
}
