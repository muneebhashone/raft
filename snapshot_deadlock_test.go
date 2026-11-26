// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"
)

// snapshotLockFSM holds the snapshot read lock from Snapshot until Release.
// Apply takes the write lock, so it cannot run until the snapshot is released.
// releaseSnapshot parks Snapshot so the test can fill fsmMutateCh first.
type snapshotLockFSM struct {
	mu              sync.RWMutex
	snapshotHeld    chan struct{}
	releaseSnapshot chan struct{}
	heldOnce        sync.Once
}

func newSnapshotLockFSM() *snapshotLockFSM {
	return &snapshotLockFSM{
		snapshotHeld:    make(chan struct{}),
		releaseSnapshot: make(chan struct{}),
	}
}

func (f *snapshotLockFSM) Apply(l *Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(l.Data)
}

func (f *snapshotLockFSM) Snapshot() (FSMSnapshot, error) {
	f.mu.RLock()
	f.heldOnce.Do(func() { close(f.snapshotHeld) })
	<-f.releaseSnapshot
	return &snapshotLockSnap{f: f}, nil
}

func (f *snapshotLockFSM) Restore(rc io.ReadCloser) error { return rc.Close() }

func (f *snapshotLockFSM) release() {
	select {
	case <-f.releaseSnapshot:
	default:
		close(f.releaseSnapshot)
	}
}

type snapshotLockSnap struct {
	f    *snapshotLockFSM
	once sync.Once
}

func (s *snapshotLockSnap) Persist(sink SnapshotSink) error { return sink.Close() }

func (s *snapshotLockSnap) Release() { s.once.Do(s.f.mu.RUnlock) }

// TestRaft_SnapshotApplyDeadlock is a regression test for the deadlock between
// takeSnapshot and applyBatch.
//
// takeSnapshot holds the FSM snapshot, then blocks on a configurationsFuture
// that only the main thread answers. applyBatch, on that same thread, used to
// wait on fsmMutateCh alone. An FSM that takes a lock in Snapshot and drops it
// in Release cannot Apply until the snapshot finishes, so the queue never
// drains and the configuration read never runs.
//
// The commit notification is held back until every extra log has been
// dispatched. One processLogs then has more batches than fsmMutateCh can
// buffer, and it stays blocked on the send after the FSM accepts a single
// batch and parks in Apply. That is the window in which Snapshot must finish.
func TestRaft_SnapshotApplyDeadlock(t *testing.T) {
	fsm := newSnapshotLockFSM()
	conf := inmemConfig(t)
	conf.SnapshotInterval = time.Hour
	conf.SnapshotThreshold = 1 << 62
	conf.MaxAppendEntries = 1

	c := MakeClusterCustom(t, &MakeClusterOpts{
		Peers:     1,
		Bootstrap: true,
		Conf:      conf,
		MakeFSMFunc: func() FSM {
			return fsm
		},
	})
	var applyWG sync.WaitGroup
	t.Cleanup(func() {
		fsm.release()
		c.Close()
	})

	leader := c.Leader()
	seed := leader.Apply([]byte("seed"), 0)
	if err := seed.Error(); err != nil {
		t.Fatal(err)
	}
	base := seed.Index()

	snapCh := make(chan SnapshotFuture, 1)
	go func() { snapCh <- leader.Snapshot() }()

	select {
	case <-fsm.snapshotHeld:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Snapshot to take the fsm lock")
	}

	// Match notifications go to a side channel so the leader keeps dispatching
	// instead of applying. One later commit then spans the whole batch.
	cmt := leader.leaderState.commitment
	cmt.Lock()
	realCommitCh := cmt.commitCh
	cmt.commitCh = make(chan struct{}, 1)
	cmt.Unlock()

	n := cap(leader.fsmMutateCh) + 2
	applyErrs := make([]error, n)
	for i := 0; i < n; i++ {
		applyWG.Add(1)
		go func(i int) {
			defer applyWG.Done()
			applyErrs[i] = leader.Apply([]byte{byte(i)}, 0).Error()
		}(i)
	}
	deadline := time.Now().Add(5 * time.Second)
	for cmt.getCommitIndex() < base+uint64(n) && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if got := cmt.getCommitIndex(); got < base+uint64(n) {
		t.Fatalf("timed out dispatching logs: commit index %d, want >= %d", got, base+uint64(n))
	}

	asyncNotifyCh(realCommitCh)

	blocked := false
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(leader.fsmMutateCh) == cap(leader.fsmMutateCh) &&
			leader.getCommitIndex() >= leader.getLastApplied()+uint64(cap(leader.fsmMutateCh))+2 &&
			stuckIn(goroutineDump(), "processLogs.func1") {
			blocked = true
			break
		}
		runtime.Gosched()
	}
	if !blocked {
		t.Fatalf("timed out waiting for applyBatch to block on a full fsmMutateCh (fsm queue %d/%d, commit %d, lastApplied %d)\n%s",
			len(leader.fsmMutateCh), cap(leader.fsmMutateCh), leader.getCommitIndex(), leader.getLastApplied(), goroutineDump())
	}

	// Further commits must reach the leader again once this batch drains.
	cmt.Lock()
	cmt.commitCh = realCommitCh
	cmt.Unlock()

	fsm.release()

	var snap SnapshotFuture
	select {
	case snap = <-snapCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for Snapshot to be accepted\n%s", goroutineDump())
	}
	errCh := make(chan error, 1)
	go func() { errCh <- snap.Error() }()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("snapshot failed: %v\n%s", err, goroutineDump())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("deadlock: Snapshot() did not return while applyBatch was blocked\n%s", goroutineDump())
	}
	meta, rc, err := snap.Open()
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
	if meta.ConfigurationIndex > meta.Index {
		t.Fatalf("snapshot configuration index %d is after snapshot index %d", meta.ConfigurationIndex, meta.Index)
	}
	if len(meta.Configuration.Servers) == 0 {
		t.Fatal("snapshot stored an empty configuration")
	}

	applyWG.Wait()
	for i, err := range applyErrs {
		if err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
}

// gatingFSM parks inside Apply so the test can fill fsmMutateCh.
type gatingFSM struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGatingFSM() *gatingFSM {
	return &gatingFSM{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (f *gatingFSM) Apply(*Log) interface{} {
	f.once.Do(func() { close(f.entered) })
	<-f.release
	return nil
}

func (f *gatingFSM) Snapshot() (FSMSnapshot, error) { return &gatingSnap{}, nil }
func (f *gatingFSM) Restore(rc io.ReadCloser) error { return rc.Close() }

func (f *gatingFSM) unlock() {
	select {
	case <-f.release:
	default:
		close(f.release)
	}
}

type gatingSnap struct{}

func (gatingSnap) Persist(sink SnapshotSink) error { return sink.Close() }
func (gatingSnap) Release()                        {}

// TestRaft_ApplyBatchConfigDuringLeadershipTransfer checks the leaderLoop rule
// from inside applyBatch: a configuration read during a transfer fails, and
// the same read is answered once the transfer flag is clear.
func TestRaft_ApplyBatchConfigDuringLeadershipTransfer(t *testing.T) {
	fsm := newGatingFSM()
	conf := inmemConfig(t)
	conf.SnapshotInterval = time.Hour
	conf.SnapshotThreshold = 1 << 62
	conf.MaxAppendEntries = 1

	c := MakeClusterCustom(t, &MakeClusterOpts{
		Peers:     1,
		Bootstrap: true,
		Conf:      conf,
		MakeFSMFunc: func() FSM {
			return fsm
		},
	})
	t.Cleanup(func() {
		fsm.unlock()
		c.Close()
	})

	leader := c.Leader()
	go func() { _ = leader.Apply([]byte("seed"), 0).Error() }()
	select {
	case <-fsm.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Apply to block")
	}

	for i := 0; i < cap(leader.fsmMutateCh)+2; i++ {
		go func(i int) { _ = leader.Apply([]byte{byte(i)}, 0).Error() }(i)
	}

	deadline := time.Now().Add(5 * time.Second)
	blocked := false
	for time.Now().Before(deadline) {
		if len(leader.fsmMutateCh) == cap(leader.fsmMutateCh) &&
			stuckIn(goroutineDump(), "processLogs.func1") {
			blocked = true
			break
		}
		runtime.Gosched()
	}
	if !blocked {
		t.Fatalf("timed out waiting for applyBatch to block\n%s", goroutineDump())
	}

	leader.setLeadershipTransferInProgress(true)
	if err := askConfiguration(leader); err != ErrLeadershipTransferInProgress {
		t.Fatalf("configuration read during transfer: got %v, want %v", err, ErrLeadershipTransferInProgress)
	}
	leader.setLeadershipTransferInProgress(false)

	if err := askConfiguration(leader); err != nil {
		t.Fatalf("configuration read while applyBatch is blocked: %v", err)
	}
}

// askConfiguration sends the request takeSnapshot sends and waits up to 5s.
func askConfiguration(r *Raft) error {
	req := &configurationsFuture{}
	req.ShutdownCh = r.shutdownCh
	req.init()
	go func() {
		select {
		case r.configurationsCh <- req:
		case <-r.shutdownCh:
			req.respond(ErrRaftShutdown)
		}
	}()
	errCh := make(chan error, 1)
	go func() { errCh <- req.Error() }()
	select {
	case err := <-errCh:
		return err
	case <-time.After(5 * time.Second):
		return fmt.Errorf("configuration request was not answered while applyBatch was blocked")
	}
}

func goroutineDump() string {
	var buf bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&buf, 2)
	return buf.String()
}

// stuckIn reports whether some goroutine is blocked in fn. A select that is
// waiting on a send is reported as "select", not "chan send".
func stuckIn(dump, fn string) bool {
	for _, g := range strings.Split(dump, "\n\n") {
		if !strings.Contains(g, fn) {
			continue
		}
		if strings.Contains(g, "[chan send]") || strings.Contains(g, "[select]") {
			return true
		}
	}
	return false
}
