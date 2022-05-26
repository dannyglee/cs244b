package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// This program should always start with 2 command line args, pingInterval and max consequtive ping failures.
// The first param determines how often the registry sends pings to the cluster nodes, the second param determines
// how many consequtive pings a node may miss before being considered failed.
func main() {
	pingIntervalSeconds := flag.Int("ping_interval", 30, "the periodic interval registry pings the cluster nodes")
	maxAllowablePingFailures := flag.Int("max_failure", 2, "the number of missing pings a node may have before being considered failed")
	flag.Parse()
	fmt.Printf("Process started at %d\n", time.Now().UnixMilli())
	wg := new(sync.WaitGroup)
	wg.Add(2)

	// Initialize registry.
	registry := Registry{}
	registry.init(*pingIntervalSeconds, *maxAllowablePingFailures)

	http.HandleFunc("/addSingleMember", registry.HandleAddSingleMember)
	http.HandleFunc("/removeSingleMember", registry.HandleRemoveSingleMember)
	http.HandleFunc("/updateGroup", registry.HandleMembershipChange)

	go func() {
		log.Fatal(http.ListenAndServe(":5000", nil))
		wg.Done()
	}()

	go func() {
		registry.monitorGroupHealth()
		wg.Done()
	}()

	wg.Wait()
}
