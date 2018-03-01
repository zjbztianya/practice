package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	Get    = "Get"
	Put    = "Put"
	Append = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqArgs
	Op    string
	Key   string
	Value string
}

type CommitReply struct {
	ReqArgs
	Err   Err
	Value string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db      map[string]string
	reqIdMp map[int64]int
	replyCh map[int]chan Op
}

func (kv *RaftKV) processCommand(command Op) {
	if reqId, ok := kv.reqIdMp[command.ClientId]; ok && reqId >= command.ReqId {
		return
	}
	if command.Op == Put {
		kv.db[command.Key] = command.Value

	} else if command.Op == Append {
		kv.db[command.Key] += command.Value
	}
	kv.reqIdMp[command.ClientId] = command.ReqId
}

func (kv *RaftKV) commitLog(command Op) bool {
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	replyCh, ok := kv.replyCh[index]
	if !ok {
		replyCh = make(chan Op, 1)
		kv.replyCh[index] = replyCh
	}
	kv.mu.Unlock()

	select {
	case reply := <-replyCh:
		//currentTerm, isLeader := kv.rf.GetState()
		//if !isLeader || term != currentTerm {
		//	reply.Err = ErrNotLeader
		//}
		if reply.ClientId != command.ClientId || reply.ReqId != command.ReqId {
			return false
		}
		return true
	case <-time.After(time.Second):
		return false
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{ReqArgs{args.ClientId, args.ReqId},
		Get, args.Key, ""}
	ok := kv.commitLog(command)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.Err = OK
		reply.WrongLeader = false
		reply.Value = ""
		kv.mu.Lock()
		reply.Value, ok = kv.db[command.Key]
		kv.reqIdMp[command.ClientId] = command.ReqId
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op{ReqArgs{args.ClientId, args.ReqId},
		args.Op, args.Key, args.Value}
	ok := kv.commitLog(command)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.Err = OK
		reply.WrongLeader = false
	}
}

func (kv *RaftKV) applyLoop() {
	for {
		select {
		case msg := <-kv.applyCh:
			op := msg.Command.(Op)
			kv.mu.Lock()
			if op.Op != Get {
				kv.processCommand(op)
			}
			replyCh, ok := kv.replyCh[msg.Index]
			if ok {
				select {
				case <-replyCh:
				default:
				}
				replyCh <- op
			}
			kv.mu.Unlock()
		}
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.replyCh = make(map[int]chan Op)
	kv.reqIdMp = make(map[int64]int)
	go kv.applyLoop()

	return kv
}
