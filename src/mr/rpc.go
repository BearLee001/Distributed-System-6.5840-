package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RequestWorkArgs struct {
	// ask for work
	TaskID   int
	TaskType int
}

type WorkInfoReply struct {
	TaskType int // 1 -> map; 2 -> reduce; 3 -> all task done; 4 -> some task is running, no available task
	FileName string
	TaskID   int // file no

	IsReAssigned bool
	NReduce      int
	NMap         int
}

// Add your RPC definitions here.
