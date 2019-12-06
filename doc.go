/*
Package asynq provides a framework for background task processing.

The Client is used to register a task to be processed at the specified time.

	client := asynq.NewClient(&asynq.RedisConfig{
	    Addr: "localhost:6379",
	})

	t := &asynq.Task{
	    Type:    "send_email",
	    Payload: map[string]interface{}{"recipient_id": 123},
	}

	err := client.Process(t, time.Now().Add(10 * time.Minute))

The Background is used to run the background processing with a given
handler with the specified number of workers.
    bg := asynq.NewBackground(20, &asynq.RedisConfig{
        Addr: "localhost:6379",
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
