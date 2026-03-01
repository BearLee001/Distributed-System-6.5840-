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
			return
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
				finalName := fmt.Sprintf("mr-out-%d", work.TaskID)
				atomicWriteFile(finalName, func(w io.Writer) error {
					for _, k := range keys {
						v2 := reducef(k, grouped[k])
						w.Write([]byte(k))
						w.Write([]byte(" "))
						w.Write([]byte(v2))
						w.Write([]byte("\n"))
					}
					return nil
				})
				reportWorkDone(2 /* reduce task*/, work.TaskID)
			}
		case work.TaskType == 3: // all task done, don't ask for help
			return

		case work.TaskType == 4: // no available tasks, waiting...
		default:
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
		finalName := fmt.Sprintf("mr-%d-%d", taskID, r)
		tmp, err := os.CreateTemp(".", fmt.Sprintf("mr-%d-%d-*", taskID, r))
		if err != nil {
			return err
		}
		tmpName := tmp.Name()
		enc := json.NewEncoder(tmp)
		for _, kv := range buckets[r] {
			if err := enc.Encode(&kv); err != nil {
				tmp.Close()
				_ = os.Remove(tmpName)
				return err
			}
		}
		if err := tmp.Close(); err != nil {
			_ = os.Remove(tmpName)
			return err
		}
		if err := os.Rename(tmpName, finalName); err != nil {
			_ = os.Remove(tmpName)
			return err
		}
	}
	return nil
}

// grouped:
// {(hello, [1, 1, 1, 1]), (world, [1, 1, 1, 1])}

func normalizeKV(taskSize int, suffix int) (map[string][]string, error) {
	grouped := make(map[string][]string)
	for i := 0; i < taskSize; i++ {
		name := fmt.Sprintf("mr-%d-%d", i, suffix)
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
			grouped[kv.Key] = append(grouped[kv.Key], kv.Value)
		}
		f.Close()
	}

	return grouped, nil
}

func atomicWriteFile(final string, write func(io.Writer) error) error {
	tmp, err := os.CreateTemp(".", "tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	bw := bufio.NewWriter(tmp)

	if err := write(bw); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}

	if err := bw.Flush(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}

	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}

	return os.Rename(tmpName, final)
}
