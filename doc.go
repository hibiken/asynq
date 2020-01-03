// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

/*
Package asynq provides a framework for background task processing.

The Client is used to register a task to be processed at the specified time.

	client := asynq.NewClient(redis)

	t := &asynq.Task{
	    Type:    "send_email",
	    Payload: map[string]interface{}{"recipient_id": 123},
	}

	err := client.Process(t, time.Now().Add(10 * time.Minute))

The Background is used to run the background processing with a given
handler with the specified number of workers.
    bg := asynq.NewBackground(redis, &asynq.Config{
        Concurrency: 20,
    })

    bg.Run(handler)

Handler is an interface that implements ProcessTask method that
takes a Task and return an error. Handler should return nil if
the processing is successful, otherwise return non-nil error
so that the task will be retried after some delay.

    type TaskHandler struct {
        // ...
    }

    func (h *TaskHandler) ProcessTask(task *asynq.Task) error {
        switch task.Type {
        case "send_email":
            // send email logic
        case "generate_thumbnail":
            // generate thumbnail image
        //...
        default:
            return fmt.Errorf("unepected task type %q", task.Type)
        }
        return nil
    }
*/
package asynq
