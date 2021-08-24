package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
import (
    "bytes"
    "log"
    "math/rand"
    "sync"
    "sync/atomic"
    "time"

    "../labgob"
    "../labrpc"
)

// import "bytes"
// import "../labgob"

//

const (
    Leader = iota
    Follower
    Candidate
)

const RFDebug = 1

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
    CommandValid  bool
    Command       interface{}
    CommandIndex  int
    Snapshot      []byte
    SnapshotValid bool
    SnapshotTerm  int
    SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    mu          sync.Mutex          // Lock to protect shared access to this peer's state
    peers       []*labrpc.ClientEnd // RPC end points of all peers
    persister   *Persister          // Object to hold this peer's persisted state
    me          int                 // this peer's index into peers[]
    dead        int32               // set by Kill()
    role        int
    currentTerm int
    voteFor     int

    log                 []logContent
    invalidCommandCount int    //用于标记无效命令的数量
    commitIndex         int    //commit的最大索引号
    lastApplied         int    //应用到状态机的索引号
    nextIndex           []int  //用来标记下一个日志的标号
    nextIndexStatus     []bool //用于标记nextIndex是否是有效的

    heartBeatChan chan struct{}
    applyChan     chan ApplyMsg
    peerSendChan  []chan struct{}
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    //for 2D
    snapshot          []byte
    lastIncludedTerm  int
    lastIncludedIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    term := rf.currentTerm
    isleader := false

    if rf.role == Leader {
        isleader = true
    }
    return term, isleader
}

func (rf *Raft) CondInstallSnapshot(SnapshotTerm int,
    SnapshotIndex int, Snapshot []byte) bool {

    return false
}

func (rf *Raft) Snapshot(CommandIndex int,
    content []byte) {

    rf.mu.Lock()
    defer rf.mu.Unlock()
    if CommandIndex <= rf.lastIncludedIndex {
        return
    }

    tmpTerm := rf.log[CommandIndex].Term

    rf.log = append([]logContent{{Term: 0, Content: 0}}, rf.log[CommandIndex-rf.lastIncludedIndex+1:]...)
    rf.snapshot = content
    rf.lastIncludedIndex = CommandIndex
    rf.lastIncludedTerm = tmpTerm
    rf.persistAndSnapshot(rf.snapshot)
    rf.DPrintf("begin to compress log.\n")
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.

func (rf *Raft) persist() {
    // Your code here (2C).
    // Example:
    // w := new(bytes.Buffer)
    // e := labgob.NewEncoder(w)
    // e.Encode(rf.xxx)
    // e.Encode(rf.yyy)
    // data := w.Bytes()
    // rf.persister.SaveRaftState(data)

    labgob.Register(logContent{})
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.voteFor)
    e.Encode(rf.commitIndex)
    e.Encode(rf.lastIncludedTerm)
    e.Encode(rf.lastIncludedIndex)

    for i := 1 + rf.lastIncludedIndex; i <= rf.commitIndex; i++ {
        e.Encode(rf.log[i-rf.lastIncludedIndex])
    }
    data := w.Bytes()
    rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistAndSnapshot(snapshot []byte) {
    labgob.Register(logContent{})
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.voteFor)
    e.Encode(rf.commitIndex)
    e.Encode(rf.lastIncludedTerm)
    e.Encode(rf.lastIncludedIndex)

    for i := 1 + rf.lastIncludedIndex; i <= rf.commitIndex; i++ {
        e.Encode(rf.log[i-rf.lastIncludedIndex])
    }

    data := w.Bytes()

    rf.persister.SaveStateAndSnapshot(data, snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
    if data == nil || len(data) < 1 { // bootstrap without any state?
        return
    }

    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var currentTerm int
    var voteFor int
    var logSize int
    var lastTerm int
    var lastIndex int
    if d.Decode(&currentTerm) != nil ||
        d.Decode(&voteFor) != nil ||
        d.Decode(&logSize) != nil ||
        d.Decode(&lastTerm) != nil || d.Decode(&lastIndex) != nil {
        rf.DPrintf("failed to decode state.\n")
        return
    }

    var logOne logContent
    for i := 1; i <= logSize && d.Decode(&logOne) == nil; i++ {
        rf.log = append(rf.log, logOne)
    }
    rf.currentTerm = currentTerm
    rf.voteFor = voteFor
    rf.commitIndex = logSize
    rf.lastIncludedTerm = lastTerm
    rf.lastIncludedIndex = lastIndex
    rf.DPrintf("reload state before shutdown.term:%d,votedFor:%d,log:%v,logSize:%d,lastIncludedTerm:%d,lastIncludedIndex:%d.\n",
        rf.currentTerm, rf.voteFor, rf.log, logSize, lastTerm, lastIndex)

    // Your code here (2C).
    // Example:
    // r := bytes.NewBuffer(data)
    // d := labgob.NewDecoder(r)
    // var xxx
    // var yyy
    // if d.Decode(&xxx) != nil ||
    //    d.Decode(&yyy) != nil {
    //   error...
    // } else {
    //   rf.xxx = xxx
    //   rf.yyy = yyy
    // }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    Term         int //candidate's term
    CandidateId  int //candidate requesting vote
    LastLogIndex int //candidate's commitIndex?
    LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
    // Your data here (2A).
    Term        int
    VoteGranted bool // is it a bool type?
}

//
// example RequestVote RPC handler.
//

type AppendEntriesArgs struct {
    Term         int
    LeaderId     int
    PrevLogIndex int
    PrevLogTerm  int
    // Entries      interface{}
    // 可能一次过传输多条日志？确实是的
    Entries      []logContent
    LeaderCommit int
}

type AppendEntriesReply struct {
    Term          int
    Success       bool
    ConflictTerm  int
    ConflictIndex int
}

type logContent struct {
    Term    int
    Apply   bool
    Content interface{}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    return ok
}

func (rf *Raft) sendAppendEntires(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntires", args, reply)
    return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

    rf.mu.Lock()
    defer rf.mu.Unlock()

    reply.Term = rf.currentTerm
    reply.VoteGranted = false

    if rf.currentTerm > args.Term {
        rf.DPrintf("candidate:%d,candidate term:%d, current term:%d ,voteFor:%d. Reject.\n", args.CandidateId, args.Term, rf.currentTerm, rf.voteFor)
        return
    }

    if rf.currentTerm == args.Term && rf.voteFor != -1 {
        rf.DPrintf("same term.but has already vote for server:%d", rf.voteFor)
        return
    }

    if rf.currentTerm < args.Term {
        rf.DPrintf("candidate %d with higher term.change my term.", args.CandidateId)
        rf.becomeFollower(args.Term)
    }

    if args.LastLogTerm < rf.getMyPrevLogTerm() || (args.LastLogTerm == rf.getMyPrevLogTerm() && args.LastLogIndex < rf.getMyPrevLogIndex()) {
        rf.DPrintf("My log is more up-to-dated. reject candidate %d.\n", args.CandidateId)
    } else {
        rf.voteFor = args.CandidateId
        reply.VoteGranted = true
        rf.confirmHeartBeart()
        rf.persist()
    }
}

func (rf *Raft) AppendEntires(args *AppendEntriesArgs, reply *AppendEntriesReply) {

    rf.mu.Lock()
    defer rf.mu.Unlock()

    reply.Term = rf.currentTerm
    reply.ConflictTerm = -1
    reply.ConflictIndex = len(rf.log) + rf.lastIncludedIndex - 1

    rf.DPrintf("dealing request from server:%d", args.LeaderId)

    if args.Term < rf.currentTerm {
        rf.DPrintf("reject appendRPC from server:%d whose term is %d, while my current term is %d.\n",
            args.LeaderId, args.Term, rf.currentTerm)
        reply.Success = false
        return
    }

    if args.Term > rf.currentTerm {
        rf.role = Follower
        rf.voteFor = -1
        rf.currentTerm = args.Term
        rf.persist()
    }

    rf.confirmHeartBeart()

    // 1) If a follower does not have prevLogIndex in its log, it should return with conflictIndex = len(log) and conflictTerm = None.
    // 2) If a follower does have prevLogIndex in its log, but the term does not match, it should return
    // conflictTerm = log[prevLogIndex].Term, and then search its log for the first index whose entry has
    // term equal to conflictTerm.
    // 3) Upon receiving a conflict response, the leader should first search its log for conflictTerm. If it finds an entry in its log with that term, it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
    // 4) If it does not find an entry with that term, it should set nextIndex = conflictIndex.

    //如果lastIndex的位置，和内容任意不匹配，返回
    //就是我当前最新日志的长度小于leader发送的PrevLogIndex或者有prevLogIndex的位置但是term不相同
    if rf.getMyPrevLogIndex() < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {

        //对应2）情况
        //找到冲突位置的term，并且返回一个符合条件的最小index，否则为默认值-1

        //这是term不相同的情况
        if rf.getMyPrevLogIndex() >= args.PrevLogIndex {
            // reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
            // 可以修改为二分法来提高检索的速度
            reply.ConflictTerm = rf.getTermByActualIndex(args.PrevLogIndex)
            for i := 1; i < len(rf.log) && rf.log[i].Term <= reply.ConflictTerm; i++ {
                if rf.log[i].Term == reply.ConflictTerm {
                    reply.ConflictIndex = rf.getActualIndexByLogIndex(i)
                    break
                }
            }
        }

        // 对应1）情况
        rf.DPrintf("reject appendRPC from server:%d whose prevLogIndex is :%d and prevLogTerm is :%d,set conflict term %d,index:%d,my log is %v\n", args.LeaderId,
            args.PrevLogIndex, args.PrevLogTerm, reply.ConflictTerm, reply.ConflictIndex, rf.log)
        return

    } else {

        //相同的位置找到之后，后面的多余去掉，不多余的直接应用即可
        //这里有问题，不能够直接删除，因为网络可能有延迟的，
        //这样会因为之前的网络包删除掉原本没有问题的数据，因此逐个匹配，完全相同的就不应用了，不相同的
        // if len(rf.log) > args.PrevLogIndex {
        //
        relativePostion := args.PrevLogIndex - rf.lastIncludedIndex

        for i := 0; i < len(args.Entries); i++ {
            tmpIndex := i + relativePostion + 1
            if tmpIndex < len(rf.log) {
                if rf.log[tmpIndex].Term == args.Entries[i].Term {
                    continue
                } else {
                    rf.log = rf.log[:tmpIndex]
                }
            }
            rf.log = append(rf.log, args.Entries[i:]...)
            break
        }

        // }

        // if len(args.Entries) != 0 {
        //     // rf.DPrintf("begin to write log to local:%v,my log is :%v", args.Entries, rf.log)
        //     rf.DPrintf("begin to write log to local:%v", args.Entries)
        //     rf.log = append(rf.log, args.Entries...)
        //     rf.DPrintf("After append log. my log is %v", rf.log)
        // }

        reply.Success = true

        //follower apply log
        if rf.commitIndex < args.LeaderCommit {
            for i := rf.commitIndex + 1; i < len(rf.log)+rf.lastIncludedIndex && i <= args.LeaderCommit; i++ {
                rf.DPrintf("begin to apply log,commitIndex:%d,LeaderCommit:%d\n", i, args.LeaderCommit)
                tmpIndex := rf.getActualIndexByLogIndex(i)
                rf.applyChan <- ApplyMsg{CommandValid: rf.log[tmpIndex].Apply, Command: rf.log[tmpIndex].Content, CommandIndex: i}
            }

            //如果长度大于leaderCommit长度，commitIndex设置为leaderCommit
            //否则设置为当前最大的长度
            if args.LeaderCommit < len(rf.log)+rf.lastIncludedIndex-1 {
                rf.commitIndex = args.LeaderCommit
            } else {
                rf.commitIndex = len(rf.log) + rf.lastIncludedIndex - 1
            }
            rf.lastApplied = rf.commitIndex
        }
        rf.persist()
    }
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    index := -1
    term := -1
    isLeader := true

    // Your code here (2B).
    term, isLeader = rf.GetState()

    if isLeader {
        rf.mu.Lock()
        defer rf.mu.Unlock()
        index = rf.getMyPrevLogIndex() + 1
        rf.DPrintf("recieve write request from client.command:%v.\n", command)
        rf.log = append(rf.log, logContent{Term: term, Apply: true, Content: command})
        rf.persist()

    }
    // return index - rf.invalidCommandCount, term, isLeader

    return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
    atomic.StoreInt32(&rf.dead, 1)
    // Your code here, if desired.
}

func (rf *Raft) killed() bool {
    z := atomic.LoadInt32(&rf.dead)
    return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {

    rf := &Raft{}

    rf.peers = peers
    rf.persister = persister
    rf.me = me
    rf.voteFor = -1
    rf.role = Follower

    rf.nextIndex = make([]int, len(peers))

    // Your initialization code here (2A, 2B, 2C).

    rf.heartBeatChan = make(chan struct{}, 1)
    rf.nextIndexStatus = make([]bool, len(rf.peers))
    rf.log = append(rf.log, logContent{Term: 0, Content: 0})

    //为了实现插入一个空命令，同时满足cfg中的一些约束，增加一个无效命令的数量，来确定命令在cfg中的index
    rf.invalidCommandCount = 0

    rf.applyChan = applyCh

    rf.peerSendChan = make([]chan struct{}, len(rf.peers))

    rf.lastIncludedIndex = 0
    rf.lastIncludedTerm = 0

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())
    rf.snapshot = persister.ReadSnapshot()

    for i := 0; i < len(rf.peers); i++ {
        if i != rf.me {
            rf.nextIndex[i] = len(rf.log)
            rf.nextIndexStatus[i] = false
        }
        rf.peerSendChan[i] = make(chan struct{}, 1)
    }

    go func() {
        // heartBeatInterval := 300
        // heartBeatTimeOut := 800 + (rand.Int63() % 800)
        // electionTimeOut := 800 + (rand.Int63() % 800)

        heartBeatInterval := 100
        heartBeatTimeOut := 300 + (rand.Int63() % 200)
        electionTimeOut := 300 + (rand.Int63() % 200)

        // heartBeatInterval := 50
        // heartBeatTimeOut := 150 + (rand.Int63() % 100)
        // electionTimeOut := 150 + (rand.Int63() % 100)

        // heartBeatInterval := 50
        // heartBeatTimeOut := 100 + (rand.Int63() % 100)
        // electionTimeOut := 100 + (rand.Int63() % 100)

        // round:=0
        heartBeatTimeOutTimer := time.NewTimer(time.Duration(heartBeatTimeOut) * time.Millisecond)
        defer heartBeatTimeOutTimer.Stop()
        // var role int
        for {
            if rf.killed() {
                return
            }
            rf.mu.Lock()
            role := rf.role
            rf.mu.Unlock()

            switch role {
            case Leader:
                rf.sendAppendEntry()
                time.Sleep(time.Duration(heartBeatInterval) * time.Millisecond)
            case Follower:

                if !heartBeatTimeOutTimer.Stop() {
                    select {
                    case <-heartBeatTimeOutTimer.C:
                    default:
                    }
                }
                heartBeatTimeOutTimer.Reset(time.Duration(heartBeatTimeOut) * time.Millisecond)
                select {
                case <-rf.heartBeatChan:
                    rf.DPrintf("recieve heartbeat from server.\n")
                case <-heartBeatTimeOutTimer.C:
                    // case<-time.After(time.Duration(heartBeatTimeOut)*time.Millisecond):
                    rf.mu.Lock()
                    rf.DPrintf("heartbeart timeout. become candidate.before election,my term is %d\n", rf.currentTerm)
                    rf.role = Candidate
                    rf.mu.Unlock()
                }
            case Candidate:
                rf.kickOffElection(electionTimeOut)
            }
        }
    }()

    return rf
}

func (rf *Raft) confirmHeartBeart() {
    rf.DPrintf("confirm heartbeat from server.")
    select {
    case rf.heartBeatChan <- struct{}{}:
    default:
    }
}

func (rf *Raft) sendAppendEntry() {

    for i := 0; i < len(rf.peers); i++ {

        if i == rf.me {
            continue
        }
        // select {
        //peerSendChan为了控制不重复进行发送的？有点忘记了；因为apply的时候可能会出现前后到达而重复写入的问题；
        //客户端不应该考虑这些事情

        // case rf.peerSendChan[i] <- struct{}{}:
        // rf.DPrintf("begin to send  log entries taks to server %d", i)
        go func(index int) {
            // defer func() {
            //     select {
            //     case <-rf.peerSendChan[index]:
            //     default:
            //     }
            //     rf.DPrintf("leave append log entries taks of server %d", index)
            // }()
            for {

                rf.mu.Lock()

                if rf.role != Leader {
                    rf.mu.Unlock()
                    return
                }

                //Part 2D - 需要增加判断是否需要发送snapshot
                //这个判断和leader实例的snapshot也相关
                lastIndex := rf.getPrevLogIndexForIndex(index)
                lastTerm := rf.getPrevLogTermForIndex(index)

                //lastIndex+1为nextIndex
                if lastIndex+1 <= rf.lastIncludedIndex {
                    //发送快照，以及进行相关的标记
                    //unlock
                    rf.DPrintf("debug\n")
                    return
                } else {

                    //匹配压缩后的日志队列位置
                    offset := lastIndex - rf.lastIncludedIndex + 1

                    args := AppendEntriesArgs{
                        rf.currentTerm,
                        rf.me,
                        lastIndex,
                        lastTerm,
                        append(make([]logContent, 0), rf.log[offset:]...),
                        rf.commitIndex,
                    }
                    reply := &AppendEntriesReply{}
                    rf.DPrintf("send append entiry to server %d,logEntries:%d\n", index, len(args.Entries))
                    if len(args.Entries) > 0 {
                        rf.DPrintf("FirstlogContent:%v\n", args.Entries[0])
                    }
                    rf.mu.Unlock()

                    ret := rf.sendAppendEntires(index, &args, reply)

                    rf.mu.Lock()

                    //疑问，这样似乎没办法阻止重复发送的问题,好像并不是个很大的问题
                    //重复发送的问题导致后面的处理有问题了，如果遇到一些需要回退索引消耗时间比较长的
                    //可能会出现非常多的线程在一起执行的

                    if !ret || rf.role != Leader {
                        rf.nextIndexStatus[index] = false
                        rf.mu.Unlock()
                        return
                    }

                    if reply.Success {

                        if rf.nextIndex[index] < args.PrevLogIndex+len(args.Entries)+1 {
                            rf.nextIndex[index] = args.PrevLogIndex + len(args.Entries) + 1
                            rf.DPrintf("status of server %d got update,nextIndex change to %d\n", index, rf.nextIndex[index])
                        }
                        rf.nextIndexStatus[index] = true

                        //如果是涉及到写入，同时，此刻主的commitid并不是满的
                        //matchIndex的作用有吗，目前还看不出来matchIndex的作用
                        rf.updateCommitIndex()
                        // if len(args.Entries)>0 && rf.commitIndex<len(rf.log)-1{
                        // 	atomic.AddInt32(writeCount)
                        // 	if atomic.LoadInt32(writeCount)>len(rf.peers)/2{
                        // 		rf.updateCommitIndex()
                        // 	}
                        // }

                        rf.mu.Unlock()
                        return
                    }

                    if reply.Term > rf.currentTerm {
                        rf.DPrintf("reply from server %d with a higher term %d, step down to be a follower,change term from %d to %d.\n", index, reply.Term, rf.currentTerm, reply.Term)
                        rf.role = Follower
                        rf.voteFor = -1
                        rf.currentTerm = reply.Term
                        rf.mu.Unlock()
                        return
                    } else {
                        // if reply.PrevLogTerm < args.PrevLogTerm {

                        //     //如果此时记载的rf.nextIndex超过rf.log最大值，所以置一个上线
                        //     if rf.nextIndex[index] > len(rf.log) {
                        //         rf.nextIndex[index] = len(rf.log) + 1
                        //     }

                        //     for reply.PrevLogTerm < rf.log[rf.nextIndex[index]-1].Term {
                        //         rf.nextIndex[index] -= 1
                        //     }
                        // } else {
                        //     rf.nextIndex[index] -= 1
                        // }

                        //reply.ConflictTerm两种结果，一种是-1，一种不是-1

                        if reply.ConflictTerm == -1 {
                            //ConflictTerm为-1，是因为没有冲突，发生在folower的prev没有日志
                            //如果小于的rf.lastIncludedIndex的会发送快照
                            rf.nextIndex[index] = reply.ConflictIndex
                        } else {

                            //如果冲突的term比快照的term还要小，只需要发快照
                            if reply.ConflictTerm < rf.lastIncludedTerm {
                                rf.nextIndex[index] = rf.lastIncludedIndex
                            } else {

                                tmpNextIndex := reply.ConflictIndex
                                searchTerm := -1
                                for i := 1; i < len(rf.log) && rf.log[i].Term <= reply.ConflictTerm; i++ {
                                    if rf.log[i].Term == reply.ConflictTerm {
                                        for i < len(rf.log) && rf.log[i].Term <= reply.ConflictTerm {
                                            i++
                                        }
                                        if rf.log[i-1].Term == reply.ConflictTerm {
                                            searchTerm = reply.ConflictTerm
                                            tmpNextIndex = i
                                        }
                                        if searchTerm == -1 {
                                            tmpNextIndex = reply.ConflictIndex
                                        }
                                        break
                                    }
                                }
                                rf.nextIndex[index] = tmpNextIndex
                            }
                        }
                        rf.DPrintf("nextIndex of server %d change to %d.\n", index, rf.nextIndex[index])

                        rf.mu.Unlock()
                    }
                }
            }

        }(i)
        // default:

    }
}

func (rf *Raft) updateCommitIndex() {

    //不对以往任何的term进行commit，只对自己的term的日志进行commit
    //每次选主的时候，都会发送一个空的日志，以满足这个条件
    if rf.getMyPrevLogTerm() == rf.currentTerm {

        if rf.commitIndex < rf.getMyPrevLogIndex() {
            rf.DPrintf("commitIndex:%d prevLogIndex:%d,try to commit log.\n", rf.commitIndex, rf.getMyPrevLogIndex())
            for i := rf.commitIndex + 1; i <= rf.getMyPrevLogIndex(); i++ {
                count := 1
                for index := range rf.peers {
                    if index == rf.me {
                        continue
                    }
                    if rf.nextIndexStatus[index] && rf.nextIndex[index]-1 >= i {
                        count++
                    }
                }

                if count > len(rf.peers)/2 {
                    rf.commitIndex++
                } else {
                    break
                }

            }
        }

        rf.leaderApplyLog()
    }
}

func (rf *Raft) leaderApplyLog() {
    for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
        rf.DPrintf("begin to apply log for for index:%d", i)
        rf.applyChan <- ApplyMsg{CommandValid: rf.log[i].Apply, Command: rf.log[i].Content, CommandIndex: i}
        rf.lastApplied++
    }
    rf.persist()
}

func (rf *Raft) kickOffElection(timeOut int64) {

    rf.mu.Lock()
    rf.voteFor = rf.me
    rf.currentTerm += 1
    rf.persist()
    args := RequestVoteArgs{
        rf.currentTerm,
        rf.me,
        rf.getMyPrevLogIndex(),
        rf.getMyPrevLogTerm(),
    }
    rf.mu.Unlock()

    timerTimeout := time.NewTimer(time.Duration(timeOut) * time.Millisecond)
    defer timerTimeout.Stop()
    electionSleep := 300 + (rand.Int63() % 200)
    // electionSleep := 150 + (rand.Int63() % 100)

    var voteCount int32 = 1

    done := make(chan struct{})
    closeTag := false

    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }

        go func(done chan struct{}, index int) {
            reply := &RequestVoteReply{}
            ret := rf.sendRequestVote(index, &args, reply)

            if ret {
                rf.mu.Lock()
                defer rf.mu.Unlock()
                rf.DPrintf("recieve vote response from server %d,getVote:%v\n", index, reply.VoteGranted)

                if rf.currentTerm < reply.Term {
                    rf.DPrintf("step down to a follower.")
                    rf.becomeFollower(reply.Term)
                    // rf.closeChan(done)
                    return
                }

                if rf.role != Candidate {
                    return
                }

                if reply.VoteGranted {
                    atomic.AddInt32(&voteCount, 1)
                }

                if atomic.LoadInt32(&voteCount) > int32(len(rf.peers)/2) {
                    rf.DPrintf("win election.become leader.")
                    rf.becomeLeader()
                    if !closeTag {
                        closeTag = rf.closeChan(done)
                    }
                    return
                }
            }
        }(done, i)
    }

    //超时或者任务完成判断
    select {
    case <-done:
        return
    case <-timerTimeout.C:
        rf.mu.Lock()
        rf.DPrintf("Election timeout. Sleep.")
        rf.voteFor = -1
        rf.mu.Unlock()
        time.Sleep(time.Duration(electionSleep) * time.Millisecond)
        return
    }

}

func (rf *Raft) closeChan(channel chan struct{}) bool {
    select {
    case _, isClose := <-channel:
        if !isClose {
            close(channel)
        }
    default:
        close(channel)
    }
    return true
}

func (rf *Raft) becomeLeader() {
    rf.role = Leader
    for i := 0; i < len(rf.peers); i++ {
        rf.nextIndex[i] = len(rf.log)
        rf.nextIndexStatus[i] = false
    }
    rf.DPrintf("reset peers' info. reset rf.nextIndex to %d\n", len(rf.log))
    rf.nextIndexStatus[rf.me] = true

    // rf.invalidCommandCount = 0
    // for _, content := range rf.log {
    //     if content.Content == -1 {
    //         rf.invalidCommandCount++
    //     }
    // }
    //成为leader之后发送一条空消息
    rf.nextIndex[rf.me] += 1
    // rf.invalidCommandCount++
    // rf.DPrintf("become leader.Send empty command.\n")
    // rf.log = append(rf.log, logContent{Term: rf.currentTerm, Apply: false, Content: -1})
    // rf.persist()
}

func (rf *Raft) becomeCandidate() {
    rf.role = Candidate
    rf.currentTerm += 1
    rf.voteFor = rf.me
    rf.persist()
}

func (rf *Raft) becomeFollower(newTerm int) {
    rf.role = Follower
    rf.currentTerm = newTerm
    rf.voteFor = -1
    rf.persist()
}

func (rf *Raft) getPrevLogIndexForIndex(i int) int {
    return rf.nextIndex[i] - 1
}
func (rf *Raft) getPrevLogTermForIndex(i int) int {
    return rf.log[rf.nextIndex[i]-1].Term
}

func (rf *Raft) getTermByActualIndex(i int) int {
    return rf.log[i-rf.lastIncludedIndex].Term
}

func (rf *Raft) getActualIndexByLogIndex(i int) int {
    return i + rf.lastIncludedIndex
}

func (rf *Raft) getMyPrevLogIndex() int {
    //2D-增加快照做对应的修改
    return len(rf.log) - 1 + rf.lastIncludedIndex
}
func (rf *Raft) getMyPrevLogTerm() int {
    //2D-增加快照做对应的修改
    return rf.log[rf.getMyPrevLogIndex()-rf.lastIncludedIndex].Term
}

func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
    if RFDebug > 0 {
        newFormat := "[server:%v role:%s term:%d commitIndex:%d] " + format
        var role string

        switch rf.role {
        case Leader:
            role = "L"
        case Follower:
            role = "F"
        case Candidate:
            role = "C"
        }

        var para []interface{}
        para = append(para, rf.me)
        para = append(para, role)
        para = append(para, rf.currentTerm)
        para = append(para, rf.commitIndex)
        if len(a) != 0 {
            para = append(para, a...)
        }
        log.Printf(newFormat, para...)
    }
    return
}
