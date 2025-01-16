package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	UUID string // 请求的唯一ID
}

type PutAppendReply struct {
	Value string
	UUID  string // 请求的唯一ID
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}

type DeleteUUIDArgs struct {
	UUID string
}

type DeleteUUIDReply struct {
}
