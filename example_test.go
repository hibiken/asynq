// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq_test

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/hibiken/asynq"
	"golang.org/x/sys/unix"
)

func ExampleServer_Run() {
	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: ":6379"},
		asynq.Config{Concurrency: 20},
	)

	h := asynq.NewServeMux()
	// ... Register handlers

	// Run blocks and waits for os signal to terminate the program.
	if err := srv.Run(h); err != nil {
		log.Fatal(err)
	}
}

func ExampleServer_Stop() {
	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: ":6379"},
		asynq.Config{Concurrency: 20},
	)

	h := asynq.NewServeMux()
	// ... Register handlers

	if err := srv.Start(h); err != nil {
		log.Fatal(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, unix.SIGTERM, unix.SIGINT)
	<-sigs // wait for termination signal

	srv.Stop()
}

func ExampleServer_Quiet() {
	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: ":6379"},
		asynq.Config{Concurrency: 20},
	)

	h := asynq.NewServeMux()
	// ... Register handlers

	if err := srv.Start(h); err != nil {
		log.Fatal(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, unix.SIGTERM, unix.SIGINT, unix.SIGTSTP)
	// Handle SIGTERM, SIGINT to exit the program.
	// Handle SIGTSTP to stop processing new tasks.
	for {
		s := <-sigs
		if s == unix.SIGTSTP {
			srv.Quiet() // stop processing new tasks
			continue
		}
		break
	}

	srv.Stop()
}

func ExampleParseRedisURI() {
	rconn, err := asynq.ParseRedisURI("redis://localhost:6379/10")
	if err != nil {
		log.Fatal(err)
	}
	r, ok := rconn.(asynq.RedisClientOpt)
	if !ok {
		log.Fatal("unexpected type")
	}
	fmt.Println(r.Addr)
	fmt.Println(r.DB)
	// Output:
	// localhost:6379
	// 10
}
