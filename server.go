package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

// Debug is a constant yay
const Debug = 0

// DPrintf is used for debugging
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Op struct for command information
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Key   string
	Value string
	Cid   int64
	Seq   int
}

// RaftKV struct for raft instance information
type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	// key-value storage
	kvStore map[string]string
	// Requests will have unique clerkID associated with them
	request map[int64]int
	result  map[int]chan Op
}

// AppendToMyLog checks for leader status and starts the Op on Raft Service
func (kv *RaftKV) AppendToMyLog(op Op) bool {

	// Go ahead and start this OP on the Raft Service
	index, _, isLeader := kv.rf.Start(op)
	// If I am not the leader return false here, The clerk needs to find the leader
	if !isLeader {
		return false
	}

	// Now if I am the leader, I have an index for the command
	kv.mu.Lock()
	ch, ok := kv.result[index]

	// if I am seeing this Op for the first time, make a chan to send the results
	if !ok {
		ch = make(chan Op)
		kv.result[index] = ch
	}

	kv.mu.Unlock()

	// Now listen to this chan, If the Op is applied (is in applyChan).
	// ApplyToStore will send  a nod on this chan for this command
	// If there is no reply in 1 sec = fail. Otherwise true

	select {
	case <-ch:
		return true
	case <-time.After(1 * time.Second):
		return false
	}
}

// Get RPC handler
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Type: "Get", Key: args.Key}
	ok := kv.AppendToMyLog(op)

	// if the op has failed, indicate it
	if !ok {
		reply.WrongLeader = true
		return
	}

	// Otherwise we can fetch from the store and send the value back
	kv.mu.Lock()

	val, exists := kv.kvStore[args.Key]

	kv.mu.Unlock()

	if exists {
		reply.Err = OK
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
	}
	// set wrong leader too
	reply.WrongLeader = false

}

// PutAppend Handler
func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Type: args.Op, Key: args.Key, Value: args.Value, Cid: args.ClerkID, Seq: args.ReqNumber}
	ok := kv.AppendToMyLog(op)

	// Here, either the applyChan indicated success or failure
	if !ok {
		// if there was a failure indicate it in reply
		reply.WrongLeader = true
		return
	}
	reply.Err = OK
	// set wrong leader too
	reply.WrongLeader = false
}

func (kv *RaftKV) saveSnapshot() {

}

func (kv *RaftKV) readSnapshot(data []byte) {

}

// Kill is implemented here
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// ApplyToStore the request to the storage/return result to client
func (kv *RaftKV) ApplyToStore() { //!modify
	for {
		msg := <-kv.applyCh

		// Got this command
		op := msg.Command.(Op)

		kv.mu.Lock()

		// if the op is to put or append
		if op.Type != "Get" {
			// Get Op SeqNum/reqNumber from my requests
			reqNum, ok := kv.request[op.Cid]
			// Check if it is indeed a newer command same clerk. And execute it if so
			if op.Seq > reqNum || !ok {

				// Update the reqNumber that was resolved for this clerk
				kv.request[op.Cid] = op.Seq

				// Make modifications to kvStore as necessary
				if op.Type == "Put" {
					kv.kvStore[op.Key] = op.Value
				} else if op.Type == "Append" {
					kv.kvStore[op.Key] += op.Value
				}

			}
		}

		// Now, we inform ApplyToMyLog which is waiting about the result
		// get the channel to reply to
		ch, ok := kv.result[msg.Index]

		if ok {
			ch <- op
		}
		kv.mu.Unlock()
	}
}

// StartKVServer is the construction script
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should kvStore snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.kvStore = make(map[string]string)
	kv.request = make(map[int64]int)
	kv.result = make(map[int]chan Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	go kv.ApplyToStore()

	return kv
}
