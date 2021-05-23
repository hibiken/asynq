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

	taskCmd.AddCommand(taskArchiveCmd)
	taskArchiveCmd.Flags().StringP("queue", "q", "", "queue to which the task belongs")
	taskArchiveCmd.Flags().StringP("id", "t", "", "id of the task")
	taskArchiveCmd.MarkFlagRequired("queue")
	taskArchiveCmd.MarkFlagRequired("id")

	taskCmd.AddCommand(taskDeleteCmd)
	taskDeleteCmd.Flags().StringP("queue", "q", "", "queue to which the task belongs")
	taskDeleteCmd.Flags().StringP("id", "t", "", "id of the task")
	taskDeleteCmd.MarkFlagRequired("queue")
	taskDeleteCmd.MarkFlagRequired("id")

	taskCmd.AddCommand(taskRunCmd)
	taskRunCmd.Flags().StringP("queue", "q", "", "queue to which the task belongs")
	taskRunCmd.Flags().StringP("id", "t", "", "id of the task")
	taskRunCmd.MarkFlagRequired("queue")
	taskRunCmd.MarkFlagRequired("id")

	taskCmd.AddCommand(taskArchiveAllCmd)
	taskArchiveAllCmd.Flags().StringP("queue", "q", "", "queue to which the tasks belong")
	taskArchiveAllCmd.Flags().StringP("state", "s", "", "state of the tasks")
	taskArchiveAllCmd.MarkFlagRequired("queue")
	taskArchiveAllCmd.MarkFlagRequired("state")

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
- active
- pending
- scheduled
- retry
- archived

List opeartion paginates the result set.
By default, the command fetches the first 30 tasks.
Use --page and --size flags to specify the page number and size.

Example: 
To list pending tasks from "default" queue, run
  asynq task ls --queue=default --state=pending

To list the tasks from the second page, run
  asynq task ls --queue=default --state=pending --page=1`,
	Run: taskList,
}

var taskCancelCmd = &cobra.Command{
	Use:   "cancel TASK_ID [TASK_ID...]",
	Short: "Cancel one or more active tasks",
	Args:  cobra.MinimumNArgs(1),
	Run:   taskCancel,
}

var taskArchiveCmd = &cobra.Command{
	Use:   "archive --queue=QUEUE --id=TASK_ID",
	Short: "Archive a task with the given id",
	Args:  cobra.NoArgs,
	Run:   taskArchive,
}

var taskDeleteCmd = &cobra.Command{
	Use:   "delete --queue=QUEUE --id=TASK_ID",
	Short: "Delete a task with the given id",
	Args:  cobra.NoArgs,
	Run:   taskDelete,
}

var taskRunCmd = &cobra.Command{
	Use:   "run --queue=QUEUE --id=TASK_ID",
	Short: "Run a task with the given id",
	Args:  cobra.NoArgs,
	Run:   taskRun,
}

var taskArchiveAllCmd = &cobra.Command{
	Use:   "archive-all --queue=QUEUE --state=STATE",
	Short: "Archive all tasks in the given state",
	Args:  cobra.NoArgs,
	Run:   taskArchiveAll,
}

var taskDeleteAllCmd = &cobra.Command{
	Use:   "delete-all --queue=QUEUE --state=STATE",
	Short: "Delete all tasks in the given state",
	Args:  cobra.NoArgs,
	Run:   taskDeleteAll,
}

var taskRunAllCmd = &cobra.Command{
	Use:   "run-all --queue=QUEUE --state=STATE",
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
	case "active":
		listActiveTasks(qname, pageNum, pageSize)
	case "pending":
		listPendingTasks(qname, pageNum, pageSize)
	case "scheduled":
		listScheduledTasks(qname, pageNum, pageSize)
	case "retry":
		listRetryTasks(qname, pageNum, pageSize)
	case "archived":
		listArchivedTasks(qname, pageNum, pageSize)
	default:
		fmt.Printf("error: state=%q is not supported\n", state)
		os.Exit(1)
	}
}

func listActiveTasks(qname string, pageNum, pageSize int) {
	i := createInspector()
	tasks, err := i.ListActiveTasks(qname, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Printf("No active tasks in %q queue\n", qname)
		return
	}
	printTable(
		[]string{"ID", "Type", "Payload"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				fmt.Fprintf(w, tmpl, t.ID(), t.Type(), t.Payload())
			}
		},
	)
}

func listPendingTasks(qname string, pageNum, pageSize int) {
	i := createInspector()
	tasks, err := i.ListPendingTasks(qname, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Printf("No pending tasks in %q queue\n", qname)
		return
	}
	printTable(
		[]string{"ID", "Type", "Payload"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				fmt.Fprintf(w, tmpl, t.ID(), t.Type(), t.Payload())
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
		[]string{"ID", "Type", "Payload", "Process In"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				processIn := fmt.Sprintf("%.0f seconds",
					t.NextProcessAt().Sub(time.Now()).Seconds())
				fmt.Fprintf(w, tmpl, t.ID(), t.Type(), t.Payload(), processIn)
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
		[]string{"ID", "Type", "Payload", "Next Retry", "Last Error", "Retried", "Max Retry"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				var nextRetry string
				if d := t.NextProcessAt().Sub(time.Now()); d > 0 {
					nextRetry = fmt.Sprintf("in %v", d.Round(time.Second))
				} else {
					nextRetry = "right now"
				}
				fmt.Fprintf(w, tmpl, t.ID(), t.Type(), t.Payload(), nextRetry, t.LastErr(), t.Retried(), t.MaxRetry())
			}
		},
	)
}

func listArchivedTasks(qname string, pageNum, pageSize int) {
	i := createInspector()
	tasks, err := i.ListArchivedTasks(qname, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Printf("No archived tasks in %q queue\n", qname)
		return
	}
	printTable(
		[]string{"ID", "Type", "Payload", "Last Failed", "Last Error"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				fmt.Fprintf(w, tmpl, t.ID(), t.Type(), t.Payload(), t.LastFailedAt(), t.LastErr())
			}
		})
}

func taskCancel(cmd *cobra.Command, args []string) {
	i := createInspector()
	for _, id := range args {
		if err := i.CancelProcessing(id); err != nil {
			fmt.Printf("error: could not send cancelation signal: %v\n", err)
			continue
		}
		fmt.Printf("Sent cancelation signal for task %s\n", id)
	}
}

func taskArchive(cmd *cobra.Command, args []string) {
	qname, err := cmd.Flags().GetString("queue")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	id, err := cmd.Flags().GetString("id")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}

	i := createInspector()
	err = i.ArchiveTask(qname, id)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("task archived")
}

func taskDelete(cmd *cobra.Command, args []string) {
	qname, err := cmd.Flags().GetString("queue")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	id, err := cmd.Flags().GetString("id")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}

	i := createInspector()
	err = i.DeleteTask(qname, id)
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
	id, err := cmd.Flags().GetString("id")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}

	i := createInspector()
	err = i.RunTask(qname, id)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("task is now pending")
}

func taskArchiveAll(cmd *cobra.Command, args []string) {
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
	case "pending":
		n, err = i.ArchiveAllPendingTasks(qname)
	case "scheduled":
		n, err = i.ArchiveAllScheduledTasks(qname)
	case "retry":
		n, err = i.ArchiveAllRetryTasks(qname)
	default:
		fmt.Printf("error: unsupported state %q\n", state)
		os.Exit(1)
	}
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("%d tasks archived\n", n)
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
	case "pending":
		n, err = i.DeleteAllPendingTasks(qname)
	case "scheduled":
		n, err = i.DeleteAllScheduledTasks(qname)
	case "retry":
		n, err = i.DeleteAllRetryTasks(qname)
	case "archived":
		n, err = i.DeleteAllArchivedTasks(qname)
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
		n, err = i.RunAllScheduledTasks(qname)
	case "retry":
		n, err = i.RunAllRetryTasks(qname)
	case "archived":
		n, err = i.RunAllArchivedTasks(qname)
	default:
		fmt.Printf("error: unsupported state %q\n", state)
		os.Exit(1)
	}
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("%d tasks are now pending\n", n)
}
