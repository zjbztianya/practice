package raftkv

const (
	OK           = "OK"
	ErrNotLeader = "ErrNotLeader"
)

type Err string

type ReqArgs struct {
	ClientId int64
	ReqId    int
}

// Put or Append
type PutAppendArgs struct {
	ReqArgs
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	ReqArgs
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
