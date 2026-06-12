package asynq_test

import (
	"context"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/hibiken/asynq/internal/base"
	"github.com/redis/go-redis/v9"
)

func TestKeyPrefix(t *testing.T) {
	// Test with custom key prefix
	customPrefix := "test-asynq"

	// Create client with custom prefix
	client := asynq.NewClient(asynq.RedisClientOpt{
		Addr:      "127.0.0.1:6379",
		DB:        1, // Use DB 1 for testing
		KeyPrefix: customPrefix,
	})
	defer client.Close()

	// Create inspector with same prefix
	inspector := asynq.NewInspector(asynq.RedisClientOpt{
		Addr:      "127.0.0.1:6379",
		DB:        1,
		KeyPrefix: customPrefix,
	})
	defer inspector.Close()

	// Clean up any existing keys first
	redisClient := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   1,
	})
	defer redisClient.Close()

	// Clear all keys with our prefix
	keys, err := redisClient.Keys(context.Background(), customPrefix+":*").Result()
	if err == nil && len(keys) > 0 {
		redisClient.Del(context.Background(), keys...)
	}

	// Verify the prefix is set correctly
	if base.GetKeyPrefix() != customPrefix {
		t.Errorf("Expected key prefix to be %q, got %q", customPrefix, base.GetKeyPrefix())
	}

	// Enqueue a task
	task := asynq.NewTask("test", nil)
	_, err = client.Enqueue(task)
	if err != nil {
		t.Fatalf("Failed to enqueue task: %v", err)
	}

	// Verify that keys are created with the custom prefix
	keys, err = redisClient.Keys(context.Background(), customPrefix+":*").Result()
	if err != nil {
		t.Fatalf("Failed to get keys: %v", err)
	}

	if len(keys) == 0 {
		t.Fatal("Expected to find keys with custom prefix, but found none")
	}

	// Verify that inspector can see the task with the custom prefix
	queues, err := inspector.Queues()
	if err != nil {
		t.Fatalf("Failed to get queues: %v", err)
	}

	if len(queues) == 0 {
		t.Fatal("Expected to find queues, but found none")
	}

	queueInfo, err := inspector.GetQueueInfo("default")
	if err != nil {
		t.Fatalf("Failed to get queue info: %v", err)
	}

	if queueInfo.Pending == 0 {
		t.Error("Expected to find pending tasks, but found none")
	}

	// Test with empty prefix (should use default)
	base.SetKeyPrefix("")
	if base.GetKeyPrefix() != "asynq" {
		t.Errorf("Expected default key prefix to be 'asynq', got %q", base.GetKeyPrefix())
	}

	// Test backward compatibility - when no KeyPrefix is specified in client options,
	// the current prefix should not be changed
	originalPrefix := base.GetKeyPrefix()
	clientWithoutPrefix := asynq.NewClient(asynq.RedisClientOpt{
		Addr: "127.0.0.1:6379",
		DB:   1,
		// No KeyPrefix specified
	})
	defer clientWithoutPrefix.Close()

	// The prefix should remain unchanged
	if base.GetKeyPrefix() != originalPrefix {
		t.Errorf("Expected prefix to remain %q when not specified, but it changed to %q", originalPrefix, base.GetKeyPrefix())
	}

	// Test with cluster client
	clusterClient := asynq.NewClient(asynq.RedisClusterClientOpt{
		Addrs:     []string{"127.0.0.1:7000"}, // This might not exist, but that's ok for testing
		KeyPrefix: "cluster-test",
	})
	defer clusterClient.Close()

	// Verify the prefix is set
	if base.GetKeyPrefix() != "cluster-test" {
		t.Errorf("Expected key prefix to be 'cluster-test', got %q", base.GetKeyPrefix())
	}

	// Test with failover client
	failoverClient := asynq.NewClient(asynq.RedisFailoverClientOpt{
		MasterName:    "mymaster",
		SentinelAddrs: []string{"127.0.0.1:26379"},
		KeyPrefix:     "failover-test",
	})
	defer failoverClient.Close()

	// Verify the prefix is set
	if base.GetKeyPrefix() != "failover-test" {
		t.Errorf("Expected key prefix to be 'failover-test', got %q", base.GetKeyPrefix())
	}
}

func TestKeyPrefixThreadSafety(t *testing.T) {
	// Test that SetKeyPrefix and GetKeyPrefix are thread-safe
	done := make(chan bool, 10)

	// Start multiple goroutines that set and get the key prefix
	for i := 0; i < 10; i++ {
		go func(prefix string) {
			defer func() { done <- true }()

			base.SetKeyPrefix(prefix)
			retrieved := base.GetKeyPrefix()

			if retrieved != prefix {
				t.Errorf("Expected prefix %q, got %q", prefix, retrieved)
			}
		}("test-" + string(rune('a'+i)))
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Test timed out")
		}
	}
}

func TestKeyGenerationWithPrefix(t *testing.T) {
	// Test that key generation functions use the correct prefix
	originalPrefix := base.GetKeyPrefix()
	defer base.SetKeyPrefix(originalPrefix) // Restore original prefix

	testPrefix := "my-custom-prefix"
	base.SetKeyPrefix(testPrefix)

	// Test queue key
	queueKey := base.PendingKey("testqueue")
	expectedQueueKey := testPrefix + ":{testqueue}:pending"
	if queueKey != expectedQueueKey {
		t.Errorf("Expected queue key %q, got %q", expectedQueueKey, queueKey)
	}

	// Test global keys
	allServersKey := base.AllServersKey()
	expectedServersKey := testPrefix + ":servers"
	if allServersKey != expectedServersKey {
		t.Errorf("Expected servers key %q, got %q", expectedServersKey, allServersKey)
	}

	allWorkersKey := base.AllWorkersKey()
	expectedWorkersKey := testPrefix + ":workers"
	if allWorkersKey != expectedWorkersKey {
		t.Errorf("Expected workers key %q, got %q", expectedWorkersKey, allWorkersKey)
	}

	// Test server info key
	serverInfoKey := base.ServerInfoKey("hostname", 1234, "server123")
	expectedServerInfoKey := testPrefix + ":servers:{hostname:1234:server123}"
	if serverInfoKey != expectedServerInfoKey {
		t.Errorf("Expected server info key %q, got %q", expectedServerInfoKey, serverInfoKey)
	}
}

func TestApplyKeyPrefixBackwardCompatibility(t *testing.T) {
	// Test that ApplyKeyPrefix preserves existing behavior when no prefix is provided
	originalPrefix := base.GetKeyPrefix()
	defer base.SetKeyPrefix(originalPrefix) // Restore original prefix

	// Set a custom prefix first
	base.SetKeyPrefix("test-prefix")

	// ApplyKeyPrefix with empty string should not change the current prefix
	base.ApplyKeyPrefix("")
	if base.GetKeyPrefix() != "test-prefix" {
		t.Errorf("Expected prefix to remain 'test-prefix' when ApplyKeyPrefix is called with empty string, got %q", base.GetKeyPrefix())
	}

	// ApplyKeyPrefix with a non-empty string should change the prefix
	base.ApplyKeyPrefix("new-prefix")
	if base.GetKeyPrefix() != "new-prefix" {
		t.Errorf("Expected prefix to change to 'new-prefix', got %q", base.GetKeyPrefix())
	}

	// Test that client creation without KeyPrefix doesn't affect global state
	originalPrefix = base.GetKeyPrefix()

	// Create client without specifying KeyPrefix
	opt := asynq.RedisClientOpt{
		Addr: "127.0.0.1:6379",
		DB:   1,
		// KeyPrefix is intentionally not set (empty string)
	}

	// This should trigger ApplyKeyPrefix("") which should not change the current prefix
	opt.MakeRedisClient()

	if base.GetKeyPrefix() != originalPrefix {
		t.Errorf("Expected prefix to remain %q after creating client without KeyPrefix, got %q", originalPrefix, base.GetKeyPrefix())
	}
}
