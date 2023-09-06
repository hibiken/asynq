// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/Kua-Fu/asynq"
	"github.com/MakeNowJust/heredoc/v2"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(taskCmd)
	taskCmd.AddCommand(taskListCmd)
	taskListCmd.Flags().StringP("queue", "q", "", "queue to inspect (required)")
	taskListCmd.Flags().StringP("state", "s", "", "state of the tasks; one of { active | pending | aggregating | scheduled | retry | archived | completed } (required)")
	taskListCmd.Flags().Int("page", 1, "page number")
	taskListCmd.Flags().Int("size", 30, "page size")
	taskListCmd.Flags().StringP("group", "g", "", "group to inspect (required for listing aggregating tasks)")
	taskListCmd.MarkFlagRequired("queue")
	taskListCmd.MarkFlagRequired("state")

	taskCmd.AddCommand(taskCancelCmd)

	taskCmd.AddCommand(taskInspectCmd)
	taskInspectCmd.Flags().StringP("queue", "q", "", "queue to which the task belongs (required)")
	taskInspectCmd.Flags().StringP("id", "i", "", "id of the task (required)")
	taskInspectCmd.MarkFlagRequired("queue")
	taskInspectCmd.MarkFlagRequired("id")

	taskCmd.AddCommand(taskArchiveCmd)
	taskArchiveCmd.Flags().StringP("queue", "q", "", "queue to which the task belongs (required)")
	taskArchiveCmd.Flags().StringP("id", "i", "", "id of the task (required)")
	taskArchiveCmd.MarkFlagRequired("queue")
	taskArchiveCmd.MarkFlagRequired("id")

	taskCmd.AddCommand(taskDeleteCmd)
	taskDeleteCmd.Flags().StringP("queue", "q", "", "queue to which the task belongs (required)")
	taskDeleteCmd.Flags().StringP("id", "i", "", "id of the task (required)")
	taskDeleteCmd.MarkFlagRequired("queue")
	taskDeleteCmd.MarkFlagRequired("id")

	taskCmd.AddCommand(taskRunCmd)
	taskRunCmd.Flags().StringP("queue", "q", "", "queue to which the task belongs (required)")
	taskRunCmd.Flags().StringP("id", "i", "", "id of the task (required)")
	taskRunCmd.MarkFlagRequired("queue")
	taskRunCmd.MarkFlagRequired("id")

	taskCmd.AddCommand(taskArchiveAllCmd)
	taskArchiveAllCmd.Flags().StringP("queue", "q", "", "queue to which the tasks belong (required)")
	taskArchiveAllCmd.Flags().StringP("state", "s", "", "state of the tasks; one of { pending | aggregating | scheduled | retry } (required)")
	taskArchiveAllCmd.MarkFlagRequired("queue")
	taskArchiveAllCmd.MarkFlagRequired("state")
	taskArchiveAllCmd.Flags().StringP("group", "g", "", "group to which the tasks belong (required for archiving aggregating tasks)")

	taskCmd.AddCommand(taskDeleteAllCmd)
	taskDeleteAllCmd.Flags().StringP("queue", "q", "", "queue to which the tasks belong (required)")
	taskDeleteAllCmd.Flags().StringP("state", "s", "", "state of the tasks; one of { pending | aggregating | scheduled | retry | archived | completed } (required)")
	taskDeleteAllCmd.MarkFlagRequired("queue")
	taskDeleteAllCmd.MarkFlagRequired("state")
	taskDeleteAllCmd.Flags().StringP("group", "g", "", "group to which the tasks belong (required for deleting aggregating tasks)")

	taskCmd.AddCommand(taskRunAllCmd)
	taskRunAllCmd.Flags().StringP("queue", "q", "", "queue to which the tasks belong (required)")
	taskRunAllCmd.Flags().StringP("state", "s", "", "state of the tasks; one of { scheduled | retry | archived } (required)")
	taskRunAllCmd.MarkFlagRequired("queue")
	taskRunAllCmd.MarkFlagRequired("state")
	taskRunAllCmd.Flags().StringP("group", "g", "", "group to which the tasks belong (required for running aggregating tasks)")
}

var taskCmd = &cobra.Command{
	Use:   "task <command> [flags]",
	Short: "Manage tasks",
	Example: heredoc.Doc(`
		$ asynq task list --queue=myqueue --state=scheduled
		$ asynq task inspect --queue=myqueue --id=7837f142-6337-4217-9276-8f27281b67d1
		$ asynq task delete --queue=myqueue --id=7837f142-6337-4217-9276-8f27281b67d1
		$ asynq task deleteall --queue=myqueue --state=archived`),
}

var taskListCmd = &cobra.Command{
	Use:     "list --queue=<queue> --state=<state> [flags]",
	Aliases: []string{"ls"},
	Short:   "List tasks",
	Long: heredoc.Doc(`
	List tasks of the given state from the specified queue.

	The --queue and --state flags are required.

	Note: For aggregating tasks, additional --group flag is required.

	List opeartion paginates the result set. By default, the command fetches the first 30 tasks.
	Use --page and --size flags to specify the page number and size.`),
	Example: heredoc.Doc(`
		$ asynq task list --queue=myqueue --state=pending
		$ asynq task list --queue=myqueue --state=aggregating --group=mygroup
		$ asynq task list --queue=myqueue --state=scheduled --page=2`),
	Run: taskList,
}

var taskInspectCmd = &cobra.Command{
	Use:   "inspect --queue=<queue> --id=<task_id>",
	Short: "Display detailed information on the specified task",
	Args:  cobra.NoArgs,
	Run:   taskInspect,
	Example: heredoc.Doc(`
		$ asynq task inspect --queue=myqueue --id=f1720682-f5a6-4db1-8953-4f48ae541d0f`),
}

var taskCancelCmd = &cobra.Command{
	Use:   "cancel <task_id> [<task_id>...]",
	Short: "Cancel one or more active tasks",
	Args:  cobra.MinimumNArgs(1),
	Run:   taskCancel,
	Example: heredoc.Doc(`
		$ asynq task cancel f1720682-f5a6-4db1-8953-4f48ae541d0f`),
}

var taskArchiveCmd = &cobra.Command{
	Use:   "archive --queue=<queue> --id=<task_id>",
	Short: "Archive a task with the given id",
	Args:  cobra.NoArgs,
	Run:   taskArchive,
	Example: heredoc.Doc(`
		$ asynq task archive --queue=myqueue --id=f1720682-f5a6-4db1-8953-4f48ae541d0f`),
}

var taskDeleteCmd = &cobra.Command{
	Use:     "delete --queue=<queue> --id=<task_id>",
	Aliases: []string{"remove", "rm"},
	Short:   "Delete a task with the given id",
	Args:    cobra.NoArgs,
	Run:     taskDelete,
	Example: heredoc.Doc(`
		$ asynq task delete --queue=myqueue --id=f1720682-f5a6-4db1-8953-4f48ae541d0f`),
}

var taskRunCmd = &cobra.Command{
	Use:   "run --queue=<queue> --id=<task_id>",
	Short: "Run a task with the given id",
	Args:  cobra.NoArgs,
	Run:   taskRun,
	Example: heredoc.Doc(`
		$ asynq task run --queue=myqueue --id=f1720682-f5a6-4db1-8953-4f48ae541d0f`),
}

var taskArchiveAllCmd = &cobra.Command{
	Use:   "archiveall --queue=<queue> --state=<state>",
	Short: "Archive all tasks in the given state",
	Args:  cobra.NoArgs,
	Run:   taskArchiveAll,
	Example: heredoc.Doc(`
		$ asynq task archiveall --queue=myqueue --state=retry
		$ asynq task archiveall --queue=myqueue --state=aggregating --group=mygroup`),
}

var taskDeleteAllCmd = &cobra.Command{
	Use:   "deleteall --queue=<queue> --state=<state>",
	Short: "Delete all tasks in the given state",
	Args:  cobra.NoArgs,
	Run:   taskDeleteAll,
	Example: heredoc.Doc(`
		$ asynq task deleteall --queue=myqueue --state=archived
		$ asynq task deleteall --queue=myqueue --state=aggregating --group=mygroup`),
}

var taskRunAllCmd = &cobra.Command{
	Use:   "runall --queue=<queue> --state=<state>",
	Short: "Run all tasks in the given state",
	Args:  cobra.NoArgs,
	Run:   taskRunAll,
	Example: heredoc.Doc(`
		$ asynq task runall --queue=myqueue --state=retry
		$ asynq task runall --queue=myqueue --state=aggregating --group=mygroup`),
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
	case "completed":
		listCompletedTasks(qname, pageNum, pageSize)
	case "aggregating":
		group, err := cmd.Flags().GetString("group")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if group == "" {
			fmt.Println("Flag --group is required for listing aggregating tasks")
			os.Exit(1)
		}
		listAggregatingTasks(qname, group, pageNum, pageSize)
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
				fmt.Fprintf(w, tmpl, t.ID, t.Type, sprintBytes(t.Payload))
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
				fmt.Fprintf(w, tmpl, t.ID, t.Type, sprintBytes(t.Payload))
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
				fmt.Fprintf(w, tmpl, t.ID, t.Type, sprintBytes(t.Payload), formatProcessAt(t.NextProcessAt))
			}
		},
	)
}

// formatProcessAt formats next process at time to human friendly string.
// If processAt time is in the past, returns "right now".
// If processAt time is in the future, returns "in xxx" where xxx is the duration from now.
func formatProcessAt(processAt time.Time) string {
	d := processAt.Sub(time.Now())
	if d < 0 {
		return "right now"
	}
	return fmt.Sprintf("in %v", d.Round(time.Second))
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
		[]string{"ID", "Type", "Payload", "Next Retry", "Last Error", "Last Failed", "Retried", "Max Retry"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				fmt.Fprintf(w, tmpl, t.ID, t.Type, sprintBytes(t.Payload), formatProcessAt(t.NextProcessAt),
					t.LastErr, formatPastTime(t.LastFailedAt), t.Retried, t.MaxRetry)
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
				fmt.Fprintf(w, tmpl, t.ID, t.Type, sprintBytes(t.Payload), formatPastTime(t.LastFailedAt), t.LastErr)
			}
		})
}

func listCompletedTasks(qname string, pageNum, pageSize int) {
	i := createInspector()
	tasks, err := i.ListCompletedTasks(qname, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Printf("No completed tasks in %q queue\n", qname)
		return
	}
	printTable(
		[]string{"ID", "Type", "Payload", "CompletedAt", "Result"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				fmt.Fprintf(w, tmpl, t.ID, t.Type, sprintBytes(t.Payload), formatPastTime(t.CompletedAt), sprintBytes(t.Result))
			}
		})
}

func listAggregatingTasks(qname, group string, pageNum, pageSize int) {
	i := createInspector()
	tasks, err := i.ListAggregatingTasks(qname, group, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Printf("No aggregating tasks in group %q \n", group)
		return
	}
	printTable(
		[]string{"ID", "Type", "Payload", "Group"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				fmt.Fprintf(w, tmpl, t.ID, t.Type, sprintBytes(t.Payload), t.Group)
			}
		},
	)
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

func taskInspect(cmd *cobra.Command, args []string) {
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
	info, err := i.GetTaskInfo(qname, id)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	printTaskInfo(info)
}

func printTaskInfo(info *asynq.TaskInfo) {
	bold := color.New(color.Bold)
	bold.Println("Task Info")
	fmt.Printf("Queue:   %s\n", info.Queue)
	fmt.Printf("ID:      %s\n", info.ID)
	fmt.Printf("Type:    %s\n", info.Type)
	fmt.Printf("State:   %v\n", info.State)
	fmt.Printf("Retried: %d/%d\n", info.Retried, info.MaxRetry)
	fmt.Println()
	fmt.Printf("Next process time: %s\n", formatNextProcessAt(info.NextProcessAt))
	if len(info.LastErr) != 0 {
		fmt.Println()
		bold.Println("Last Failure")
		fmt.Printf("Failed at:     %s\n", formatPastTime(info.LastFailedAt))
		fmt.Printf("Error message: %s\n", info.LastErr)
	}
}

func formatNextProcessAt(processAt time.Time) string {
	if processAt.IsZero() || processAt.Unix() == 0 {
		return "n/a"
	}
	if processAt.Before(time.Now()) {
		return "now"
	}
	return fmt.Sprintf("%s (in %v)", processAt.Format(time.UnixDate), processAt.Sub(time.Now()).Round(time.Second))
}

// formatPastTime takes t which is time in the past and returns a user-friendly string.
func formatPastTime(t time.Time) string {
	if t.IsZero() || t.Unix() == 0 {
		return ""
	}
	return t.Format(time.UnixDate)
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
	case "aggregating":
		group, err := cmd.Flags().GetString("group")
		if err != nil {
			fmt.Printf("error: %v\n", err)
			os.Exit(1)
		}
		if group == "" {
			fmt.Println("error: Flag --group is required for aggregating tasks")
			os.Exit(1)
		}
		n, err = i.ArchiveAllAggregatingTasks(qname, group)
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
	case "completed":
		n, err = i.DeleteAllCompletedTasks(qname)
	case "aggregating":
		group, err := cmd.Flags().GetString("group")
		if err != nil {
			fmt.Printf("error: %v\n", err)
			os.Exit(1)
		}
		if group == "" {
			fmt.Println("error: Flag --group is required for aggregating tasks")
			os.Exit(1)
		}
		n, err = i.DeleteAllAggregatingTasks(qname, group)
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
	case "aggregating":
		group, err := cmd.Flags().GetString("group")
		if err != nil {
			fmt.Printf("error: %v\n", err)
			os.Exit(1)
		}
		if group == "" {
			fmt.Println("error: Flag --group is required for aggregating tasks")
			os.Exit(1)
		}
		n, err = i.RunAllAggregatingTasks(qname, group)
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
