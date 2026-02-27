package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"fmt"
	"os"

	"6.5840/mr"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so sockname\n")
		os.Exit(1)
	}

	mapf, reducef := mr.LoadPlugin(os.Args[1])

	mr.Worker(os.Args[2], mapf, reducef)
}
