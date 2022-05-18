package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

func main() {
	fmt.Printf("Process started at %d\n", time.Now().UnixMilli())
	wg := new(sync.WaitGroup)
	wg.Add(2)

	// NodeId to port mapping.
	activeNodes := map[string]string{}

	var lock sync.Mutex

	http.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		defer lock.Unlock()
		nodeId := r.FormValue("nodeId")
		url := r.FormValue("url")
		bytes, _ := json.Marshal(activeNodes)
		activeNodes[nodeId] = url
		fmt.Println(fmt.Sprintf("Node %s started serving at %s", nodeId, url))
		w.Write(bytes)
	})

	go func() {
		log.Fatal(http.ListenAndServe(":5000", nil))
		wg.Done()
	}()

	wg.Wait()
}
