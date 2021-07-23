// Replicated State Machine

// Based on raftexample key-value store provided by etcd
// extended key value store to fully formed state machine, with
// a defined API for providing a replicated bank app

package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
)

// a replicated state machine backed by raft
type rsm struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	kvStore     map[string]float32 // current committed key-value pairs
	snapshotter *snap.Snapshotter
}

// Operation enum
type Operation int

const (
	CreateUser Operation = iota // create a new user with specified name and balance
	DeleteUser           = iota // delete an existing user with specfied name
	Transfer             = iota // transact between two specified users a specified amount
	Withdraw             = iota // withdraw a specified amount from an existing user
	Deposit              = iota // deposit a specified amount to an existing user
)

// an action on the state machine
type action struct {
	Op     Operation // the Operation type
	Source string    // the user to create/delete, or the source of a transaction
	Target string    // for transactional Operations, target of the transaction
	Val    float32   // for Create, corresponding balance, for transactions, the amount to transfer
}

func newKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) *rsm {
	s := &rsm{proposeC: proposeC, kvStore: make(map[string]float32), snapshotter: snapshotter}
	snapshot, err := s.loadSnapshot()
	if err != nil {
		log.Panic(err)
	}
	if snapshot != nil {
		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			log.Panic(err)
		}
	}
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	return s
}

func (s *rsm) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var keys []string
	for k := range s.kvStore {
		keys = append(keys, k)
	}
	return keys
}

func (s *rsm) Lookup(key string) (float32, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.kvStore[key]
	return v, ok
}

// Propose a specified operation, encoding this operation
// type and its resulting parameter set and sending to the proposal channel
func (s *rsm) ProposeDelete(user string) {
	var buf bytes.Buffer

	delAction := action{Op: DeleteUser, Source: user}

	if err := gob.NewEncoder(&buf).Encode(delAction); err != nil {
		log.Fatal(err)
	} else {
		log.Printf("proposing delete %v", delAction)
	}

	s.proposeC <- buf.String()
}

// Commit a delete operation of the specified user
func (s *rsm) CommitDelete(user string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Printf("committing delete %s", user)
	delete(s.kvStore, user)
}

// Propose a specified operation, encoding this operation
// type and its resulting parameter set and sending to the proposal channel
func (s *rsm) ProposeTransfer(source string, target string, amount float32) {
	var buf bytes.Buffer

	txnAction := action{Op: Transfer, Source: source, Target: target, Val: amount}

	if err := gob.NewEncoder(&buf).Encode(txnAction); err != nil {
		log.Fatal(err)
	} else {
		log.Printf("proposing transfer %v", txnAction)
	}

	s.proposeC <- buf.String()
}

// Commit a transaction operation of the specified amount and source/target
func (s *rsm) CommitTransfer(source string, target string, amount float32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Printf("committing transfer %s %s %f", source, target, amount)
	s.kvStore[source] -= amount
	s.kvStore[target] += amount
}

// Propose a create operation
func (s *rsm) ProposeCreate(source string, amount float32) {
	var buf bytes.Buffer

	createAction := action{Op: CreateUser, Source: source, Val: amount}

	if err := gob.NewEncoder(&buf).Encode(createAction); err != nil {
		log.Fatal(err)
	} else {
		log.Printf("proposing create %v", createAction)
	}

	s.proposeC <- buf.String()
}

func (s *rsm) CommitCreate(source string, amount float32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Printf("committing create %s %f", source, amount)
	s.kvStore[source] = amount
}

// Read commit loop, indefinitely waits on commit channel and applies
// any updates which come in.
func (s *rsm) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		if commit == nil {
			// signaled to load snapshot
			snapshot, err := s.loadSnapshot()
			if err != nil {
				log.Panic(err)
			}
			if snapshot != nil {
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
					log.Panic(err)
				}
			}
			continue
		}

		// Apply the commit, determining the appropriate
		// action based on the operation type of the decoded data
		for _, data := range commit.data {
			var action action
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&action); err != nil {
				log.Fatalf("raftstore: could not decode message (%v)", err)
			} else {
				log.Printf("raftstore: received %v", action)
			}

			switch action.Op {
			case CreateUser:
				log.Printf("commiting create of %v", action)
				s.CommitCreate(action.Source, action.Val)
			case DeleteUser:
				log.Printf("commiting delete of %v", action)
				s.CommitDelete(action.Source)
			case Transfer:
				log.Printf("commiting transfer of %v", action)
				s.CommitTransfer(action.Source, action.Target, action.Val)
			default:
				log.Panicf("raftstore: unknown operation type %d", action.Op)
			}

		}
		close(commit.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *rsm) getSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.kvStore)
}

func (s *rsm) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *rsm) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]float32
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kvStore = store
	return nil
}
