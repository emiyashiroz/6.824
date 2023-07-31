package raft

const (
	RaftRoleFollower = iota
	RaftRoleLeader
	RaftRoleCandidate
	StatusNotVoted = -1

	MinTimeout = 200
	MaxTimeout = 500
)
