package asynq

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v7"
)

// Redis keys
const (
	queuePrefix  = "asynq:queues:"         // LIST - asynq:queues:<qname>
	defaultQueue = queuePrefix + "default" // LIST
	allQueues    = "asynq:queues"          // SET
	scheduled    = "asynq:scheduled"       // ZSET
	retry        = "asynq:retry"           // ZSET
	dead         = "asynq:dead"            // ZSET
	inProgress   = "asynq:in_progress"     // SET
)

var errDequeueTimeout = errors.New("blocking dequeue operation timed out")

// rdb encapsulates the interactions with redis server.
type rdb struct {
	client *redis.Client
}

func newRDB(opt *RedisOpt) *rdb {
	client := redis.NewClient(&redis.Options{
		Addr:     opt.Addr,
		Password: opt.Password,
		DB:       opt.DB,
	})
	return &rdb{client}
}

// enqueue inserts the given task to the end of the queue.
// It also adds the queue name to the "all-queues" list.
func (r *rdb) enqueue(msg *taskMessage) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not marshal %+v to json: %v", msg, err)
	}
	qname := queuePrefix + msg.Queue
	pipe := r.client.Pipeline()
	pipe.SAdd(allQueues, qname)
	pipe.LPush(qname, string(bytes))
	_, err = pipe.Exec()
	if err != nil {
		return fmt.Errorf("could not enqueue the task %+v to %q: %v", msg, qname, err)
	}
	return nil
}

// dequeue blocks until there is a task available to be processed,
// once a task is available, it adds the task to "in progress" list
// and returns the task.
func (r *rdb) dequeue(qname string, timeout time.Duration) (*taskMessage, error) {
	data, err := r.client.BRPopLPush(qname, inProgress, timeout).Result()
	if err == redis.Nil {
		return nil, errDequeueTimeout
	}
	if err != nil {
		return nil, fmt.Errorf("command `BRPOPLPUSH %q %q %v` failed: %v", qname, inProgress, timeout, err)
	}
	var msg taskMessage
	err = json.Unmarshal([]byte(data), &msg)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal %v to json: %v", data, err)
	}
	fmt.Printf("[DEBUG] perform task %+v from %s\n", msg, qname)
	return &msg, nil
}

// remove deletes all elements equal to msg from a redis list with the given key.
func (r *rdb) remove(key string, msg *taskMessage) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not marshal %+v to json: %v", msg, err)
	}
	// NOTE: count ZERO means "remove all elements equal to val"
	err = r.client.LRem(key, 0, string(bytes)).Err()
	if err != nil {
		return fmt.Errorf("command `LREM %s 0 %s` failed: %v", key, string(bytes), err)
	}
	return nil
}

// schedule adds the task to the zset to be processd at the specified time.
func (r *rdb) schedule(zset string, processAt time.Time, msg *taskMessage) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not marshal %+v to json: %v", msg, err)
	}
	score := float64(processAt.Unix())
	err = r.client.ZAdd(zset, &redis.Z{Member: string(bytes), Score: score}).Err()
	if err != nil {
		return fmt.Errorf("command `ZADD %s %.1f %s` failed: %v", zset, score, string(bytes), err)
	}
	return nil
}

const maxDeadTask = 100
const deadExpirationInDays = 90

// kill sends the taskMessage to "dead" set.
// It also trims the sorted set by timestamp and set size.
func (r *rdb) kill(msg *taskMessage) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not marshal %+v to json: %v", msg, err)
	}
	now := time.Now()
	pipe := r.client.Pipeline()
	pipe.ZAdd(dead, &redis.Z{Member: string(bytes), Score: float64(now.Unix())})
	limit := now.AddDate(0, 0, -deadExpirationInDays).Unix() // 90 days ago
	pipe.ZRemRangeByScore(dead, "-inf", strconv.Itoa(int(limit)))
	pipe.ZRemRangeByRank(dead, 0, -maxDeadTask) // trim the set to 100
	_, err = pipe.Exec()
	return err
}

// moveAll moves all tasks from src list to dst list.
func (r *rdb) moveAll(src, dst string) error {
	script := redis.NewScript(`
	local len = redis.call("LLEN", KEYS[1])
	for i = len, 1, -1 do
		redis.call("RPOPLPUSH", KEYS[1], KEYS[2])
	end
	return len
	`)
	_, err := script.Run(r.client, []string{src, dst}).Result()
	return err
}

// forward moves all tasks with a score less than the current unix time
// from the given zset to the default queue.
// TODO(hibiken): Find a better method name that reflects what this does.
func (r *rdb) forward(from string) error {
	script := redis.NewScript(`
	local msgs = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
	for _, msg in ipairs(msgs) do
		redis.call("ZREM", KEYS[1], msg)
		redis.call("SADD", KEYS[2], KEYS[3])
		redis.call("LPUSH", KEYS[3], msg)
	end
	return msgs
	`)
	now := float64(time.Now().Unix())
	res, err := script.Run(r.client, []string{from, allQueues, defaultQueue}, now).Result()
	fmt.Printf("[DEBUG] got %d tasks from %q\n", len(res.([]interface{})), from)
	return err
}
