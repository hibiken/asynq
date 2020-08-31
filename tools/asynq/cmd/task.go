// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/hibiken/asynq"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(taskCmd)
	taskCmd.AddCommand(taskListCmd)
	taskListCmd.Flags().StringP("queue", "q", "", "queue to inspect")
	taskListCmd.Flags().StringP("state", "s", "", "state of the tasks to inspect")
	taskListCmd.Flags().Int("page", 1, "page number")
	taskListCmd.Flags().Int("size", 30, "page size")
	taskListCmd.MarkFlagRequired("queue")
	taskListCmd.MarkFlagRequired("state")

	taskCmd.AddCommand(taskCancelCmd)

	taskCmd.AddCommand(taskKillCmd)
	taskKillCmd.Flags().StringP("queue", "q", "", "queue to which the task belongs")
	taskKillCmd.Flags().StringP("key", "k", "", "key of the task")
	taskKillCmd.MarkFlagRequired("queue")
	taskKillCmd.MarkFlagRequired("key")

	taskCmd.AddCommand(taskDeleteCmd)
	taskDeleteCmd.Flags().StringP("queue", "q", "", "queue to which the task belongs")
	taskDeleteCmd.Flags().StringP("key", "k", "", "key of the task")
	taskDeleteCmd.MarkFlagRequired("queue")
	taskDeleteCmd.MarkFlagRequired("key")

	taskCmd.AddCommand(taskRunCmd)
	taskRunCmd.Flags().StringP("queue", "q", "", "queue to which the task belongs")
	taskRunCmd.Flags().StringP("key", "k", "", "key of the task")
	taskRunCmd.MarkFlagRequired("queue")
	taskRunCmd.MarkFlagRequired("key")

	taskCmd.AddCommand(taskKillAllCmd)
	taskKillAllCmd.Flags().StringP("queue", "q", "", "queue to which the tasks belong")
	taskKillAllCmd.Flags().StringP("state", "s", "", "state of the tasks")
	taskKillAllCmd.MarkFlagRequired("queue")
	taskKillAllCmd.MarkFlagRequired("state")

	taskCmd.AddCommand(taskDeleteAllCmd)
	taskDeleteAllCmd.Flags().StringP("queue", "q", "", "queue to which the tasks belong")
	taskDeleteAllCmd.Flags().StringP("state", "s", "", "state of the tasks")
	taskDeleteAllCmd.MarkFlagRequired("queue")
	taskDeleteAllCmd.MarkFlagRequired("state")

	taskCmd.AddCommand(taskRunAllCmd)
	taskRunAllCmd.Flags().StringP("queue", "q", "", "queue to which the tasks belong")
	taskRunAllCmd.Flags().StringP("state", "s", "", "state of the tasks")
	taskRunAllCmd.MarkFlagRequired("queue")
	taskRunAllCmd.MarkFlagRequired("state")
}

var taskCmd = &cobra.Command{
	Use:   "task",
	Short: "Manage tasks",
}

var taskListCmd = &cobra.Command{
	Use:   "ls --queue=QUEUE --state=STATE",
	Short: "List tasks",
	Long: `List tasks of the given state from the specified queue.

The value for the state flag should be one of:
- in-progress
- enqueued
- scheduled
- retry
- dead

List opeartion paginates the result set.
By default, the command fetches the first 30 tasks.
Use --page and --size flags to specify the page number and size.

Example: 
To list enqueued tasks from "default" queue, run
  asynq task ls --queue=default --state=enqueued

To list the tasks from the second page, run
  asynq task ls --queue=default --state=enqueued --page=1`,
	Run: taskList,
}

var taskCancelCmd = &cobra.Command{
	Use:   "cancel TASK_ID [TASK_ID...]",
	Short: "Cancel one or more in-progress tasks",
	Args:  cobra.MinimumNArgs(1),
	Run:   taskCancel,
}

var taskKillCmd = &cobra.Command{
	Use:   "kill --queue=QUEUE --key=KEY",
	Short: "Kill a task with the given key",
	Args:  cobra.NoArgs,
	Run:   taskKill,
}

var taskDeleteCmd = &cobra.Command{
	Use:   "delete --queue=QUEUE --key=KEY",
	Short: "Delete a task with the given key",
	Args:  cobra.NoArgs,
	Run:   taskDelete,
}

var taskRunCmd = &cobra.Command{
	Use:   "run --queue=QUEUE --key=KEY",
	Short: "Run a task with the given key",
	Args:  cobra.NoArgs,
	Run:   taskRun,
}

var taskKillAllCmd = &cobra.Command{
	Use:   "kill-all --queue=QUEUE --state=STATE",
	Short: "Kill all tasks in the given state",
	Args:  cobra.NoArgs,
	Run:   taskKillAll,
}

var taskDeleteAllCmd = &cobra.Command{
	Use:   "delete-all --queue=QUEUE --key=KEY",
	Short: "Delete all tasks in the given state",
	Args:  cobra.NoArgs,
	Run:   taskDeleteAll,
}

var taskRunAllCmd = &cobra.Command{
	Use:   "run-all --queue=QUEUE --key=KEY",
	Short: "Run all tasks in the given state",
	Args:  cobra.NoArgs,
	Run:   taskRunAll,
}

func taskList(cmd *cobra.Command, args []string) {
	qname, err := cmd.Flags().GetString("queue")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	state, err := cmd.Flags().GetString("state")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	pageNum, err := cmd.Flags().GetInt("page")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	pageSize, err := cmd.Flags().GetInt("size")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	switch state {
	case "in-progress":
		listInProgressTasks(qname, pageNum, pageSize)
	case "enqueued":
		listEnqueuedTasks(qname, pageNum, pageSize)
	case "scheduled":
		listScheduledTasks(qname, pageNum, pageSize)
	case "retry":
		listRetryTasks(qname, pageNum, pageSize)
	case "dead":
		listDeadTasks(qname, pageNum, pageSize)
	default:
		fmt.Printf("error: state=%q is not supported\n", state)
		os.Exit(1)
	}
}

func listInProgressTasks(qname string, pageNum, pageSize int) {
	i := createInspector()
	tasks, err := i.ListInProgressTasks(qname, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Printf("No in-progress tasks in %q queue\n", qname)
		return
	}
	printTable(
		[]string{"ID", "Type", "Payload"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				fmt.Fprintf(w, tmpl, t.ID, t.Type, t.Payload)
			}
		},
	)
}

func listEnqueuedTasks(qname string, pageNum, pageSize int) {
	i := createInspector()
	tasks, err := i.ListEnqueuedTasks(qname, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Printf("No enqueued tasks in %q queue\n", qname)
		return
	}
	printTable(
		[]string{"ID", "Type", "Payload"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				fmt.Fprintf(w, tmpl, t.ID, t.Type, t.Payload)
			}
		},
	)
}

func listScheduledTasks(qname string, pageNum, pageSize int) {
	i := createInspector()
	tasks, err := i.ListScheduledTasks(qname, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Printf("No scheduled tasks in %q queue\n", qname)
		return
	}
	printTable(
		[]string{"Key", "Type", "Payload", "Process In"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				processIn := fmt.Sprintf("%.0f seconds",
					t.NextEnqueueAt.Sub(time.Now()).Seconds())
				fmt.Fprintf(w, tmpl, t.Key(), t.Type, t.Payload, processIn)
			}
		},
	)
}

func listRetryTasks(qname string, pageNum, pageSize int) {
	i := createInspector()
	tasks, err := i.ListRetryTasks(qname, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Printf("No retry tasks in %q queue\n", qname)
		return
	}
	printTable(
		[]string{"Key", "Type", "Payload", "Next Retry", "Last Error", "Retried", "Max Retry"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				var nextRetry string
				if d := t.NextEnqueueAt.Sub(time.Now()); d > 0 {
					nextRetry = fmt.Sprintf("in %v", d.Round(time.Second))
				} else {
					nextRetry = "right now"
				}
				fmt.Fprintf(w, tmpl, t.Key(), t.Type, t.Payload, nextRetry, t.ErrorMsg, t.Retried, t.MaxRetry)
			}
		},
	)
}

func listDeadTasks(qname string, pageNum, pageSize int) {
	i := createInspector()
	tasks, err := i.ListDeadTasks(qname, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Printf("No dead tasks in %q queue\n", qname)
		return
	}
	printTable(
		[]string{"Key", "Type", "Payload", "Last Failed", "Last Error"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				fmt.Fprintf(w, tmpl, t.Key(), t.Type, t.Payload, t.LastFailedAt, t.ErrorMsg)
			}
		})
}

func taskCancel(cmd *cobra.Command, args []string) {
	r := createRDB()
	for _, id := range args {
		err := r.PublishCancelation(id)
		if err != nil {
			fmt.Printf("error: could not send cancelation signal: %v\n", err)
			continue
		}
		fmt.Printf("Sent cancelation signal for task %s\n", id)
	}
}

func taskKill(cmd *cobra.Command, args []string) {
	qname, err := cmd.Flags().GetString("queue")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	key, err := cmd.Flags().GetString("key")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}

	i := createInspector()
	err = i.KillTaskByKey(qname, key)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("task transitioned to dead state")
}

func taskDelete(cmd *cobra.Command, args []string) {
	qname, err := cmd.Flags().GetString("queue")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	key, err := cmd.Flags().GetString("key")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}

	i := createInspector()
	err = i.DeleteTaskByKey(qname, key)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("task deleted")
}

func taskRun(cmd *cobra.Command, args []string) {
	qname, err := cmd.Flags().GetString("queue")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	key, err := cmd.Flags().GetString("key")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}

	i := createInspector()
	err = i.EnqueueTaskByKey(qname, key)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("task transitioned to pending state")
}

func taskKillAll(cmd *cobra.Command, args []string) {
	qname, err := cmd.Flags().GetString("queue")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	state, err := cmd.Flags().GetString("state")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}

	i := createInspector()
	var n int
	switch state {
	case "scheduled":
		n, err = i.KillAllScheduledTasks(qname)
	case "retry":
		n, err = i.KillAllRetryTasks(qname)
	default:
		fmt.Printf("error: unsupported state %q\n", state)
		os.Exit(1)
	}
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("%d tasks transitioned to dead state\n", n)
}

func taskDeleteAll(cmd *cobra.Command, args []string) {
	qname, err := cmd.Flags().GetString("queue")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	state, err := cmd.Flags().GetString("state")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}

	i := createInspector()
	var n int
	switch state {
	case "scheduled":
		n, err = i.DeleteAllScheduledTasks(qname)
	case "retry":
		n, err = i.DeleteAllRetryTasks(qname)
	case "dead":
		n, err = i.DeleteAllDeadTasks(qname)
	default:
		fmt.Printf("error: unsupported state %q\n", state)
		os.Exit(1)
	}
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("%d tasks deleted\n", n)
}

func taskRunAll(cmd *cobra.Command, args []string) {
	qname, err := cmd.Flags().GetString("queue")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	state, err := cmd.Flags().GetString("state")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}

	i := createInspector()
	var n int
	switch state {
	case "scheduled":
		n, err = i.EnqueueAllScheduledTasks(qname)
	case "retry":
		n, err = i.EnqueueAllRetryTasks(qname)
	case "dead":
		n, err = i.EnqueueAllDeadTasks(qname)
	default:
		fmt.Printf("error: unsupported state %q\n", state)
		os.Exit(1)
	}
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("%d tasks transitioned to pending state\n", n)
}
