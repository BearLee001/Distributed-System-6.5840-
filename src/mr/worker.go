package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		work, err := requestTasksFromCoordinator()
		if err != nil {
			fmt.Println("Failed to ask for works")
		}
		switch {
		case work.TaskType == 1: // map task
			input_file := work.FileName
			content, err := os.ReadFile(input_file)
			if err != nil {
				fmt.Println("Failed to ask for works!!!")
			}
			kvs := mapf(input_file /* not used */, string(content))
			taskID := work.TaskID

			// write kvs to file
			// Note: Not production-safe, but efficient for now.
			if writeKVs2File(kvs, work.NReduce, taskID) == nil {
				reportWorkDone(1 /* map task */, taskID)
			}
		case work.TaskType == 2: // reduce task
			// handle tmp-[mapID]-taskID
			grouped, err := normalizeKV(work.NMap, work.TaskID)
			keys := make([]string, 0, len(grouped))
			for k := range grouped {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			if err != nil {
				fmt.Println(err)
				return
			} else {
				of, _ := os.Create(fmt.Sprintf("mr-out-%d", work.TaskID))
				bw := bufio.NewWriter(of)
				for _, k := range keys {
					v2 := reducef(k, grouped[k]) // (hello, [1, 1, 1, 1]) -> 4
					bw.WriteString(k)
					bw.WriteByte(' ')
					bw.WriteString(v2)
					bw.WriteByte('\n')
				}
				bw.Flush()
				of.Close()
				reportWorkDone(2 /* reduce task*/, work.TaskID)
			}
		case work.TaskType == 3: // all task done, don't ask for help

		case work.TaskType == 4: // no available tasks, waiting...

		default:
			fmt.Println(work)
		}
	}
}

func requestTasksFromCoordinator() (WorkInfoReply, error) {
	args := RequestWorkArgs{}
	reply := WorkInfoReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		return reply, nil
	} else {
		// should never go here
		fmt.Printf("call failed!\n")
		return WorkInfoReply{}, fmt.Errorf("call failed")
	}
}

func reportWorkDone(taskType int, taskID int) (WorkInfoReply, error) {
	args := RequestWorkArgs{
		TaskType: taskType,
		TaskID:   taskID,
	}
	reply := WorkInfoReply{}

	ok := call("Coordinator.ReportWorkDone", &args, &reply)
	if ok {
		return reply, nil
	} else {
		fmt.Printf("call failed!\n")
		return WorkInfoReply{}, fmt.Errorf("call failed")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

func writeKVs2File(kvs []KeyValue, n int, taskID int) error {
	buckets := make([][]KeyValue, n)
	for _, kv := range kvs {
		r := ihash(kv.Key) % n
		buckets[r] = append(buckets[r], kv)
	}
	for r := 0; r < n; r++ {
		name := fmt.Sprintf("tmp-%d-%d", taskID, r)

		f, err := os.Create(name)
		if err != nil {
			return err
		}

		enc := json.NewEncoder(f)
		for _, kv := range buckets[r] {
			if err := enc.Encode(&kv); err != nil {
				f.Close()
				return err
			}
		}
		f.Close()
	}
	return nil
}

// grouped:
// {(hello, [1, 1, 1, 1]), (world, [1, 1, 1, 1])}

func normalizeKV(taskSize int, suffix int) (map[string][]string, error) {
	grouped := make(map[string][]string)
	for i := 0; i < taskSize; i++ {
		name := fmt.Sprintf("tmp-%d-%d", i, suffix)
		f, err := os.Open(name)
		if err != nil {
			fmt.Println("failed to open file: ", name)
			return grouped, err
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				return nil, fmt.Errorf("decode %s: %w", name, err)
			}
			if kv.Value != "1" {
				f.Close()
				return nil, fmt.Errorf("unexpected value in %s: key=%q value=%q", name, kv.Key, kv.Value)
			}
			grouped[kv.Key] = append(grouped[kv.Key], kv.Value)
		}
		f.Close()
	}

	return grouped, nil
}
