package asynq

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"
)

func retryTask(rdb *rdb, msg *taskMessage, err error) {
	if msg.Retried >= msg.Retry {
		fmt.Println("[DEBUG] Retry exhausted!!!")
		if err := rdb.kill(msg); err != nil {
			log.Printf("[ERROR] could not add task %+v to 'dead' set\n", err)
		}
		return
	}
	retryAt := time.Now().Add(delaySeconds((msg.Retried)))
	fmt.Printf("[DEBUG] Retrying the task in %v\n", retryAt.Sub(time.Now()))
	msg.Retried++
	msg.ErrorMsg = err.Error()
	if err := rdb.schedule(retry, retryAt, msg); err != nil {
		// TODO(hibiken): Not sure how to handle this error
		log.Printf("[ERROR] could not add msg %+v to 'retry' set: %v\n", msg, err)
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
