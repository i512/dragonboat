package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/statemachine"
)

// ExampleOnDiskStateMachine is a simple on-disk state machine that can simulate errors
type ExampleOnDiskStateMachine struct {
	data map[string]string
	// Simulate disk failure
	shouldFail bool
}

func NewExampleOnDiskStateMachine() *ExampleOnDiskStateMachine {
	return &ExampleOnDiskStateMachine{
		data: make(map[string]string),
	}
}

func (sm *ExampleOnDiskStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	if sm.shouldFail {
		return 0, fmt.Errorf("disk failure: cannot open state machine")
	}
	return 0, nil
}

func (sm *ExampleOnDiskStateMachine) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	if sm.shouldFail {
		return nil, fmt.Errorf("disk failure: cannot write to disk")
	}

	for _, entry := range entries {
		// Simple key-value store
		sm.data[string(entry.Cmd)] = fmt.Sprintf("value_%d", entry.Index)
	}

	return entries, nil
}

func (sm *ExampleOnDiskStateMachine) Lookup(query interface{}) (interface{}, error) {
	if sm.shouldFail {
		return nil, fmt.Errorf("disk failure: cannot read from disk")
	}

	key, ok := query.(string)
	if !ok {
		return nil, fmt.Errorf("invalid query type")
	}

	value, exists := sm.data[key]
	if !exists {
		return nil, fmt.Errorf("key not found")
	}

	return value, nil
}

func (sm *ExampleOnDiskStateMachine) Sync() error {
	if sm.shouldFail {
		return fmt.Errorf("disk failure: cannot sync to disk")
	}
	return nil
}

func (sm *ExampleOnDiskStateMachine) PrepareSnapshot() (interface{}, error) {
	if sm.shouldFail {
		return nil, fmt.Errorf("disk failure: cannot prepare snapshot")
	}
	return sm.data, nil
}

func (sm *ExampleOnDiskStateMachine) SaveSnapshot(ctx interface{}, w io.Writer, stopc <-chan struct{}) error {
	if sm.shouldFail {
		return fmt.Errorf("disk failure: cannot save snapshot")
	}
	return nil
}

func (sm *ExampleOnDiskStateMachine) RecoverFromSnapshot(r io.Reader, stopc <-chan struct{}) error {
	if sm.shouldFail {
		return fmt.Errorf("disk failure: cannot recover from snapshot")
	}
	return nil
}

func (sm *ExampleOnDiskStateMachine) Close() error {
	if sm.shouldFail {
		return fmt.Errorf("disk failure: cannot close state machine")
	}
	return nil
}

func main() {
	// Create NodeHost configuration
	nhConfig := config.NodeHostConfig{
		WALDir:         "./wal",
		NodeHostDir:    "./data",
		RTTMillisecond: 200,
		RaftAddress:    "localhost:63001",
	}

	// Create NodeHost
	nh, err := dragonboat.NewNodeHost(nhConfig)
	if err != nil {
		log.Fatalf("failed to create nodehost: %v", err)
	}
	defer nh.Close()

	// Define initial members
	initialMembers := map[uint64]string{
		1: "localhost:63001",
	}

	// Create state machine factory
	createSM := func(clusterID uint64, nodeID uint64) statemachine.IOnDiskStateMachine {
		return NewExampleOnDiskStateMachine()
	}

	// Configure recovery function
	cfg := config.Config{
		ReplicaID: 1,
		// Add the recovery function
		Recover: func(err error) {
			log.Printf("Recovery function called with error: %v", err)

			// Example recovery logic:
			// 1. Check if the error is due to disk issues
			if strings.Contains(err.Error(), "disk failure") {
				log.Println("Detected disk failure, attempting recovery...")

				// 2. Try to fix the underlying issue
				// For example, check disk space, remount filesystem, etc.
				if fixDiskIssue() {
					log.Println("Disk issue fixed, restarting replica...")

					// 3. Restart the replica
					// Note: In a real implementation, you would need to:
					// - Wait for the node to be fully destroyed
					// - Create a new state machine instance
					// - Start the replica again
					restartReplica(nh, createSM)
				} else {
					log.Println("Failed to fix disk issue, alerting admin...")
					// Send alert to admin
				}
			} else {
				log.Println("Unknown error, alerting admin...")
				// Send alert to admin
			}
		},
	}

	// Start the replica
	err = nh.StartOnDiskReplica(initialMembers, false, createSM, cfg)
	if err != nil {
		log.Fatalf("failed to start replica: %v", err)
	}

	log.Println("Replica started successfully with error recovery enabled")
	log.Println("If the state machine encounters a disk error, it will:")
	log.Println("1. Call the Recover function instead of panicking")
	log.Println("2. Stop the replica gracefully")
	log.Println("3. Allow other replicas to continue operating")

	// Simulate a disk failure after some time
	time.Sleep(2 * time.Second)
	log.Println("Simulating disk failure...")

	// Try to make a proposal (this will trigger the error)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a session for the proposal
	session := nh.GetNoOPSession(1)
	_, err = nh.SyncPropose(ctx, session, []byte("test command"))
	if err != nil {
		log.Printf("Proposal failed as expected: %v", err)
	}

	// Wait for recovery to complete
	time.Sleep(5 * time.Second)
	log.Println("Example completed")
}

// fixDiskIssue simulates fixing a disk issue
func fixDiskIssue() bool {
	log.Println("Attempting to fix disk issue...")
	// In a real implementation, this would:
	// - Check disk space
	// - Remount filesystem if needed
	// - Run fsck if needed
	// - etc.

	// Simulate successful fix
	time.Sleep(1 * time.Second)
	log.Println("Disk issue fixed successfully")
	return true
}

// restartReplica demonstrates how to restart a replica after recovery
func restartReplica(nh *dragonboat.NodeHost, createSM func(uint64, uint64) statemachine.IOnDiskStateMachine) {
	log.Println("Restarting replica...")

	// In a real implementation, you would:
	// 1. Create a new state machine instance
	// 2. Start the replica again with the same configuration

	// For this example, we'll just log the restart
	log.Println("Replica restarted successfully")
}
