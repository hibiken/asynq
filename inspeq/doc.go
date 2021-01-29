// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

/*
Package inspeq provides helper types and functions to inspect queues and tasks managed by Asynq.

Inspector is used to query and mutate the state of queues.

    inspector := inspeq.New(asynq.RedisConnOpt{Addr: "localhost:6379"})

    // Query tasks from a queue.
    archived, err := inspector.ListArchivedTasks("my_queue")
	if err != nil {
		// handle error
	}

    // Take action on a task.
    for _, t := range archived {
        if err := inspector.RunTaskByKey(t.Key()); err != nil {
            // handle error
        }
    }

    // Take action on all tasks of a specified state.
    n, err := inspector.DeleteAllRetryTasks("my_queue")
    if err != nil {
        // handle error
    }
*/
package inspeq
