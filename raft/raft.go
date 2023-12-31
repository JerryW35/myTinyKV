// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	rand2 "math/rand"
	"sort"
	"time"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout       int
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hard, conf, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	if c.peers == nil {
		c.peers = conf.Nodes
	}
	r := Raft{
		id:      c.ID,
		Term:    hard.Term,
		Vote:    hard.Vote,
		RaftLog: newLog(c.Storage),
		Prs:     make(map[uint64]*Progress),
		State:   StateFollower,
		votes:   make(map[uint64]bool),
		msgs:    nil,
		Lead:    0,

		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,

		//for leader
		heartbeatElapsed: 0,
		electionElapsed:  0,
		//for 3A
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
	for _, id := range c.peers {
		r.Prs[id] = &Progress{}
	}

	r.resetTimeout()
	return &r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		return false
	}
	appendMsg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: make([]*pb.Entry, 0),
		Commit:  r.RaftLog.committed,
	}
	// the entire expected to append or cover to the follower
	nextEntries := r.RaftLog.getEntries(prevLogIndex+1, 0)
	for i := range nextEntries {
		appendMsg.Entries = append(appendMsg.Entries, &nextEntries[i])
	}
	r.msgs = append(r.msgs, appendMsg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		From:    r.id,
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)

}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.leaderTick()
	case StateCandidate:
		r.candidateTick()
	case StateFollower:
		r.followerTick()
	}
}
func (r *Raft) leaderTick() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		err := r.Step(pb.Message{
			From:    r.id,
			To:      r.id,
			MsgType: pb.MessageType_MsgBeat,
		})
		if err != nil {
			return
		}
	}
}
func (r *Raft) candidateTick() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		err := r.Step(pb.Message{
			From:    r.id,
			To:      r.id,
			MsgType: pb.MessageType_MsgHup,
		})
		if err != nil {
			return
		}
	}

}
func (r *Raft) followerTick() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		err := r.Step(pb.Message{
			From:    r.id,
			To:      r.id,
			MsgType: pb.MessageType_MsgHup,
		})
		if err != nil {
			return
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A)
	if term > r.Term {
		r.Vote = None
	}
	r.Lead = lead
	r.Term = term
	r.State = StateFollower
	r.electionElapsed = 0
	r.resetTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.State = StateCandidate

	r.Vote = r.id
	r.votes[r.id] = true

	//r.Lead = None
	r.electionElapsed = 0
	r.resetTimeout()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.Lead = r.id
	r.State = StateLeader

	for id := range r.Prs {
		//according to paper,for each server,
		//index of the next log entry to send to that server
		//(initialized to leader last log index + 1)
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
		//for each server, index of highest log entry
		//known to be replicated on server
		//(initialized to 0, increases monotonically)
		r.Prs[id].Match = 0
	}
	err := r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{}},
	})
	if err != nil {
		return
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleStartElection(m) //
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m) //
		case pb.MessageType_MsgRequestVote:
			r.handleVoteRequest(m) //
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m) //
		case pb.MessageType_MsgTimeoutNow:
			r.handleTimeoutNowRequest(m) //

		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleStartElection(m) //
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m) //
		case pb.MessageType_MsgRequestVote:
			r.handleVoteRequest(m) //
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m) //
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m) //
		case pb.MessageType_MsgTimeoutNow:
			r.handleTimeoutNowRequest(m) //
		}

	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			r.boardcastHeartBeat(m) //
		case pb.MessageType_MsgPropose:
			r.proposeMsg(m) //
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m) //
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m) //
		case pb.MessageType_MsgRequestVote:
			r.handleVoteRequest(m) //
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartBeatResponse(m) //
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	//
	// Your Code Here (2A).
	res := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}
	res.Reject = true
	if m.Term < r.Term {
		r.msgs = append(r.msgs, res)
		return
	}
	preLogIndex := m.Index
	preLogTerm := m.LogTerm
	r.becomeFollower(m.Term, m.From)
	//follower is out of date
	//Reply false if log doesn't contain an entry at prevLogIndex
	//whose term matches prevLogTerm
	//If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it
	if preLogIndex > r.RaftLog.LastIndex() ||
		r.RaftLog.getTerm(preLogIndex) != preLogTerm {
		// get current conflict index
		res.Index = r.RaftLog.LastIndex()
		// if follower has some dirty logs, then find the last log that is not conflict with leader
		if r.RaftLog.LastIndex() >= preLogIndex {
			conflictTerm := r.RaftLog.getTerm(preLogIndex)
			for _, entry := range r.RaftLog.entries {
				if entry.Term == conflictTerm {
					//find the last same entry index
					res.Index = entry.Index - 1
					break
				}
			}
		}
	} else {
		// prevLogIndex no conflict
		if len(m.Entries) > 0 {
			index, newLogIndex := m.Index+1, m.Index+1
			//The for loop then starts at m.Index+1 and, for each log entry that the leader wants to replicate,
			//checks whether the tenure of the follower's log entry at the same index is equal to the tenure of the leader's log entry.
			//If the tenure is not equal, then a log inconsistency is found at that index and the loop is exited.
			//At this point idx points to the index of the first log entry where the inconsistency was found.
			//
			for ; index < r.RaftLog.LastIndex() && index <= m.Entries[len(m.Entries)-1].Index; index++ {
				term, _ := r.RaftLog.Term(index)
				if term != m.Entries[index-newLogIndex].Term {
					break
				}
			}
			if index-newLogIndex != uint64(len(m.Entries)) {
				r.RaftLog.truncate(index)
				r.RaftLog.appendNewEntry(m.Entries[index-newLogIndex:])
				r.RaftLog.stabled = min(r.RaftLog.stabled, index-1)
			}
		}
		//update the commitIndex
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
		}
		res.Reject = false
		res.Index = m.Index + uint64(len(m.Entries))
		res.LogTerm = r.RaftLog.getTerm(res.Index)
	}
	r.msgs = append(r.msgs, res)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	heartBeatResponse := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	if r.Term <= m.Term && r.State != StateFollower {
		r.becomeFollower(m.Term, m.From)
	}
	r.msgs = append(r.msgs, heartBeatResponse)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}
func (r *Raft) handleStartElection(m pb.Message) {
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	// send requestVote
	for id := range r.Prs {
		if id == r.id {
			continue
		}

		r.msgs = append(r.msgs, pb.Message{
			From:    r.id,
			To:      id,
			Term:    r.Term,
			MsgType: pb.MessageType_MsgRequestVote,
			Index:   r.RaftLog.LastIndex(),
			LogTerm: r.RaftLog.LastTerm(),
		})
	}
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}
func (r *Raft) handleVoteRequest(m pb.Message) {
	res := pb.Message{
		From:    r.id,
		To:      m.From,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Term:    r.Term,
	}
	// if not follower, then check the term
	if r.State != StateFollower {
		if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
		} else {
			res.Reject = true
		}
	}

	if r.State == StateFollower {
		if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
		}
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)

		// if candidate has greater term, reject
		// if had been voted, reject
		// if candidate's lastLogTerm < r.Term, reject
		// if candidate's lastLogTerm = r.Term but
		// candidate's lastLogIndex < r.lastLogIndex, reject
		if m.Term < r.Term || (r.Vote != None && r.Vote != m.From) ||
			lastTerm > m.LogTerm ||
			(lastTerm == m.LogTerm && lastIndex > m.Index) {
			res.Reject = true
		} else {
			r.Vote = m.From
		}
	}

	r.msgs = append(r.msgs, res)
}
func (r *Raft) handleVoteResponse(m pb.Message) {
	// count votes
	r.votes[m.From] = !m.Reject
	count := 0
	for _, vote := range r.votes {
		if vote {
			count++
		}
	}

	if !m.Reject {
		//win the election
		if count >= len(r.Prs)/2+1 {
			r.becomeLeader()
		}
	} else {
		// check term
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		}
		// lose the election
		if len(r.votes)-count >= len(r.Prs)/2+1 {
			r.becomeFollower(r.Term, None)
		}
	}

}
func (r *Raft) handleHeartBeatResponse(m pb.Message) {
	if m.Reject {
		r.becomeFollower(m.Term, None)
	} else {
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	}
}

// for leader
func (r *Raft) boardcastHeartBeat(m pb.Message) {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
	r.heartbeatElapsed = 0
}
func (r *Raft) proposeMsg(m pb.Message) {
	//firstly append the entry to leader's entries
	r.appendEntry(m.Entries)

	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	} else {
		r.broadcastAppendEntry()
	}
}
func (r *Raft) broadcastAppendEntry() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	//for leader
	if m.Reject {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		} else {
			// has conflict
			r.Prs[m.From].Next = m.Index + 1
			r.sendAppend(m.From)
		}
		return
	}
	if r.Prs[m.From].checkUpdate(m.Index) {
		if r.checkCommit() {
			r.broadcastAppendEntry()
		}
	}

}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) resetTimeout() {
	rand2.Seed(time.Now().UnixNano())
	//[0,n)+electionTimeout
	//[electionTimeout,electionTimeout*2)
	r.randomElectionTimeout = r.electionTimeout + rand2.Intn(r.electionTimeout)
}

// Returns all logs between [start, end), end = 0 means return all logs from start.
func (l *RaftLog) getEntries(start uint64, end uint64) []pb.Entry {
	if end == 0 {
		end = l.LastIndex() + 1
	}
	start, end = start-l.dummyIndex, end-l.dummyIndex
	return l.entries[start:end]
}
func (pr *Progress) checkUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		pr.Next = pr.Match + 1
		updated = true
	}
	return updated
}

// checkCommit check if there are new logs to commit
func (r *Raft) checkCommit() bool {
	matchArray := make(uint64Slice, 0)
	for _, progress := range r.Prs {
		matchArray = append(matchArray, progress.Match)
	}
	// Get the median of all node matches, which is the log index that is replicated by most nodes
	sort.Sort(sort.Reverse(matchArray))
	majority := len(r.Prs)/2 + 1
	toCommitIndex := matchArray[majority-1]
	// Check if we can commit toCommitIndex
	return r.RaftLog.maybeCommit(toCommitIndex, r.Term)
}

// maybeCommit Check if a log that is replicated by a majority of nodes needs to be committed
func (l *RaftLog) maybeCommit(toCommit, term uint64) bool {
	commitTerm, _ := l.Term(toCommit)
	if toCommit > l.committed && commitTerm == term {
		// 只有当该日志被大多数节点复制（函数调用保证），并且日志索引大于当前的commitIndex（Condition 1）
		// 并且该日志是当前任期内创建的日志（Condition 2），才可以提交这条日志
		// 【注】为了一致性，Raft 永远不会通过计算副本的方式提交之前任期的日志，只能通过提交当前任期的日志一并提交之前所有的日志
		l.committed = toCommit
		return true
	}
	return false
}
func (r *Raft) handleTimeoutNowRequest(m pb.Message) {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	//start election
	if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup}); err != nil {
		log.Panic(err)
	}
}
func (l *RaftLog) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	lastIndex := l.LastIndex() - l.dummyIndex
	return l.entries[lastIndex].Term
}
func (l *RaftLog) truncate(startIndex uint64) {
	if len(l.entries) > 0 {
		l.entries = l.entries[:startIndex-l.dummyIndex]
	}
}
func (l *RaftLog) appendNewEntry(ents []*pb.Entry) uint64 {
	for i := range ents {
		l.entries = append(l.entries, *ents[i])
	}
	return l.LastIndex()
}
func (l *RaftLog) isUpToDate(index, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && index >= l.LastIndex())
}
func (r *Raft) appendEntry(entries []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex() // leader 最后一条日志的索引
	for i := range entries {
		// 设置新日志的索引和任期
		entries[i].Index = lastIndex + uint64(i) + 1
		entries[i].Term = r.Term
		if entries[i].EntryType == pb.EntryType_EntryConfChange {
			r.PendingConfIndex = entries[i].Index
		}
	}
	r.RaftLog.appendNewEntry(entries)
}
