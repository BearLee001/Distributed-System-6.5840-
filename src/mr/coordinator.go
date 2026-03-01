package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	allMaps       map[int]TaskInfo
	inProcessMaps map[int]TaskState

	allReduces       map[int]TaskInfo
	inProcessReduces map[int]TaskState

	gMutex sync.Mutex

	nMap    int
	nReduce int
}

type TaskInfo struct {
	taskID   int
	taskType int
	fileName string
}
type TaskState struct {
	startTime time.Time
	taskID    int
	taskType  int
	fileName  string
}

// RequestTask
// NOTE: coordinator should not call it.(RPC)
func (c *Coordinator) RequestTask(args *RequestWorkArgs, reply *WorkInfoReply) error {
	c.gMutex.Lock()
	defer c.gMutex.Unlock()
	if len(c.allMaps) > 0 {
		// assign a map task
		tmp1, e1 := c.assignTask(c.nMap, c.allMaps, c.inProcessMaps)
		if e1 == nil {
			*reply = tmp1
			return nil
		} else {
			fmt.Println("Unexpected state!!!(3)")
		}
	} else if len(c.inProcessMaps) > 0 {
		// check time out
		tmp, e := c.reAssignTask(c.nMap, c.inProcessMaps)
		if e != nil {
			*reply = WorkInfoReply{
				TaskType: 4,
			}
		} else {
			*reply = tmp
		}
		return nil
	} else if len(c.allReduces) > 0 || len(c.inProcessReduces) > 0 {
		// assign a reduce task
		tmp1, e1 := c.assignTask(c.nReduce, c.allReduces, c.inProcessReduces)
		if e1 == nil {
			*reply = tmp1
			return nil
		}
		// reassign
		tmp2, e2 := c.reAssignTask(c.nReduce, c.inProcessReduces)
		if e2 != nil {
			*reply = WorkInfoReply{
				TaskType: 4,
			}
		} else {
			*reply = tmp2
		}
		return nil
	}
	// all work done
	*reply = WorkInfoReply{
		TaskType: 3,
	}
	return nil
}

// ReportWorkDone
// NOTE: coordinator should not call it.(RPC)
func (c *Coordinator) ReportWorkDone(args *RequestWorkArgs, reply *WorkInfoReply) error {
	c.gMutex.Lock()
	defer c.gMutex.Unlock()
	switch args.TaskType {
	case 1: // map task
		_, ok := c.inProcessMaps[args.TaskID]
		if ok {
			delete(c.inProcessMaps, args.TaskID)
		} else {
			return fmt.Errorf("unexpected state!!!(1)")
		}
	case 2: // reduce task
		_, ok := c.inProcessReduces[args.TaskID]
		if ok {
			delete(c.inProcessReduces, args.TaskID)
		} else {
			return fmt.Errorf("unexpected state!!!(2)")
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.gMutex.Lock()
	defer c.gMutex.Unlock()
	ret := len(c.allReduces) == 0 &&
		len(c.inProcessReduces) == 0 &&
		len(c.allMaps) == 0 &&
		len(c.inProcessMaps) == 0
	if ret {
		fmt.Println("All tasks have been completed. Exiting the process.")
	}
	return ret
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.initAllTasks(files, nReduce)
	c.server(sockname)
	return &c
}

// helper function
func (c *Coordinator) initAllTasks(files []string, nReduce int) {
	// one file -> one map task
	c.allMaps = make(map[int]TaskInfo)
	c.inProcessMaps = make(map[int]TaskState)
	c.inProcessReduces = make(map[int]TaskState)
	c.allReduces = make(map[int]TaskInfo)
	c.nMap = len(files)
	c.nReduce = nReduce

	for i := 0; i < len(files); i++ {
		c.allMaps[i] = TaskInfo{taskID: i, taskType: 1, fileName: files[i]}
	}
	for i := 0; i < nReduce; i++ {
		c.allReduces[i] = TaskInfo{taskID: i, taskType: 2, fileName: "dont_use_me"}
	}
}

// helper function
func (c *Coordinator) reAssignTask(sz int, mp map[int]TaskState) (WorkInfoReply, error) {
	for i := 0; i < sz; i++ {
		task, ok := mp[i]
		if ok && time.Since(task.startTime) > 10*time.Second {
			// reset the start time
			task.startTime = time.Now()
			return WorkInfoReply{
				TaskID:       task.taskID,
				TaskType:     task.taskType,
				FileName:     task.fileName,
				NReduce:      c.nReduce,
				NMap:         c.nMap,
				IsReAssigned: true,
			}, nil
		}
	}
	return WorkInfoReply{}, fmt.Errorf("not found")
}

// helper function
func (c *Coordinator) assignTask(sz int, mp1 map[int]TaskInfo, mp2 map[int]TaskState) (WorkInfoReply, error) {
	for i := 0; i < sz; i++ {
		task, ok := mp1[i]
		if ok {
			result := WorkInfoReply{
				TaskID:       task.taskID,
				TaskType:     task.taskType,
				FileName:     task.fileName,
				NReduce:      c.nReduce,
				NMap:         c.nMap,
				IsReAssigned: false,
			}
			delete(mp1, i)
			mp2[i] = TaskState{
				taskID:    task.taskID,
				taskType:  task.taskType,
				fileName:  task.fileName,
				startTime: time.Now(),
			}
			return result, nil
		}
	}
	return WorkInfoReply{}, fmt.Errorf("not found")
}
