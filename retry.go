package asynq

import (
	"log"
	"math"
	"math/rand"
	"time"
)

func retryTask(rdb *rdb, msg *taskMessage, err error) {
	msg.ErrorMsg = err.Error()
	if msg.Retried >= msg.Retry {
		log.Printf("[WARN] Retry exhausted for task(Type: %q, ID: %v)\n", msg.Type, msg.ID)
		if err := rdb.kill(msg); err != nil {
			log.Printf("[ERROR] Could not add task %+v to 'dead'\n", err)
		}
		return
	}
	retryAt := time.Now().Add(delaySeconds((msg.Retried)))
	log.Printf("[INFO] Retrying task(Type: %q, ID: %v) in %v\n", msg.Type, msg.ID, retryAt.Sub(time.Now()))
	msg.Retried++
	if err := rdb.schedule(retry, retryAt, msg); err != nil {
		log.Printf("[ERROR] Could not add msg %+v to 'retry': %v\n", msg, err)
		return
	}
}

// delaySeconds returns a number seconds to delay before retrying.
// Formula taken from https://github.com/mperham/sidekiq.
func delaySeconds(count int) time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	s := int(math.Pow(float64(count), 4)) + 15 + (r.Intn(30) * (count + 1))
	return time.Duration(s) * time.Second
}
