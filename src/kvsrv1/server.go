package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	mp map[string]KVentry
}

type KVentry struct {
	value   string
	version rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.mp = make(map[string]KVentry)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.mp[args.Key]
	if ok {
		reply.Value = v.value
		reply.Err = rpc.OK
		reply.Version = v.version
	} else {
		reply.Value = ""
		reply.Err = rpc.ErrNoKey
	}
}

// Put Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.mp[args.Key]
	if ok && v.version == args.Version || !ok && args.Version == 0 {
		// **match version** or **first put**
		reply.Err = rpc.OK
		kv.mp[args.Key] = KVentry{value: args.Value, version: args.Version + 1}
	} else {
		if ok {
			reply.Err = rpc.ErrVersion
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}
}

// StartKVServer You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
