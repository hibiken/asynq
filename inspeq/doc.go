// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

/*
Package inspeq provides helper types and functions to inspect queues and tasks managed by Asynq.

Inspector is used to query and mutate the state of queues and tasks.

Example:

    inspector := inspeq.New(asynq.RedisClientOpt{Addr: "localhost:6379"})

    tasks, err := inspector.ListArchivedTasks("my-queue")

    for _, t := range tasks {
        if err := inspector.DeleteTaskByKey(t.Key()); err != nil {
            // handle error
        }
    }
*/
package inspeq
