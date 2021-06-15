package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
)

// Clerk Struct with information of clerk/client
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkID      int64
	reqNumber    int
	leaderID     int
	prevLeaderID int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// MakeClerk - a construction function for the client/clerk
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkID = nrand()
	ck.reqNumber = 0
	ck.leaderID = 0

	return ck
}

// Get request initiator
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	// set up args, send the key
	ck.reqNumber++
	args := GetArgs{Key: key}

	// First try the previous leader
	reply := GetReply{WrongLeader: false, Err: OK, Value: ""}
	ok := ck.servers[ck.prevLeaderID].Call("RaftKV.Get", &args, &reply)

	if ok && !reply.WrongLeader {

		if reply.Err == ErrNoKey {
			return ""
		}

		return reply.Value
	}

	// Otherwise we need to cycle servers

	// TODO check from previous test files
	// We need to cycle through all the servers to find the leader
	for ; ; ck.leaderID = (ck.leaderID + 1) % len(ck.servers) {
		reply := GetReply{WrongLeader: false, Err: OK, Value: ""}
		ok := ck.servers[ck.leaderID].Call("RaftKV.Get", &args, &reply)

		if ok && !reply.WrongLeader {
			// set the previous leader
			ck.prevLeaderID = ck.leaderID
			if reply.Err == ErrNoKey {
				return ""
			}

			return reply.Value
		}
	}
}

// PutAppend request initiator
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	// set up args, send the key
	ck.reqNumber++
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClerkID: ck.clerkID, ReqNumber: ck.reqNumber}

	// First try the previous leader
	reply := PutAppendReply{WrongLeader: false, Err: OK}
	ok := ck.servers[ck.prevLeaderID].Call("RaftKV.PutAppend", &args, &reply)

	if ok && !reply.WrongLeader {
		return
	}

	// Otherwise we need to cycle the servers

	for ; ; ck.leaderID = (ck.leaderID + 1) % len(ck.servers) {
		reply := PutAppendReply{WrongLeader: false, Err: OK}
		ok := ck.servers[ck.leaderID].Call("RaftKV.PutAppend", &args, &reply)

		if ok && !reply.WrongLeader {
			ck.prevLeaderID = ck.leaderID
			return
		}
	}
}

// Put RPC Caller
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append RPC Caller
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
