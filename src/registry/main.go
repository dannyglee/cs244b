package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
)

func main() {
	wg := new(sync.WaitGroup)
	wg.Add(2)

	// NodeId to port mapping.
	activeNodes := map[string]string{}

	http.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		nodeId := r.FormValue("nodeId")
		portNumber := r.FormValue("port")
		bytes, _ := json.Marshal(activeNodes)
		activeNodes[nodeId] = fmt.Sprintf("http://localhost:%s", portNumber)
		fmt.Println(fmt.Sprintf("Node %s started serving at port %s", nodeId, portNumber))
		w.Write(bytes)
	})

	go func() {
		log.Fatal(http.ListenAndServe(":5000", nil))
		wg.Done()
	}()

	wg.Wait()
}
