// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/Kua-Fu/asynq"
	"github.com/MakeNowJust/heredoc/v2"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(cronCmd)
	cronCmd.AddCommand(cronListCmd)
	cronCmd.AddCommand(cronHistoryCmd)
	cronHistoryCmd.Flags().Int("page", 1, "page number")
	cronHistoryCmd.Flags().Int("size", 30, "page size")
}

var cronCmd = &cobra.Command{
	Use:   "cron <command> [flags]",
	Short: "Manage cron",
	Example: heredoc.Doc(`
		$ asynq cron ls
		$ asynq cron history 7837f142-6337-4217-9276-8f27281b67d1`),
}

var cronListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List cron entries",
	Run:     cronList,
}

var cronHistoryCmd = &cobra.Command{
	Use:   "history <entry_id> [<entry_id>...]",
	Short: "Show history of each cron tasks",
	Args:  cobra.MinimumNArgs(1),
	Run:   cronHistory,
	Example: heredoc.Doc(`
		$ asynq cron history 7837f142-6337-4217-9276-8f27281b67d1
		$ asynq cron history 7837f142-6337-4217-9276-8f27281b67d1 bf6a8594-cd03-4968-b36a-8572c5e160dd
		$ asynq cron history 7837f142-6337-4217-9276-8f27281b67d1 --size=100
		$ asynq cron history 7837f142-6337-4217-9276-8f27281b67d1 --page=2`),
}

func cronList(cmd *cobra.Command, args []string) {
	inspector := createInspector()

	entries, err := inspector.SchedulerEntries()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(entries) == 0 {
		fmt.Println("No scheduler entries")
		return
	}

	// Sort entries by spec.
	sort.Slice(entries, func(i, j int) bool {
		x, y := entries[i], entries[j]
		return x.Spec < y.Spec
	})

	cols := []string{"EntryID", "Spec", "Type", "Payload", "Options", "Next", "Prev"}
	printRows := func(w io.Writer, tmpl string) {
		for _, e := range entries {
			fmt.Fprintf(w, tmpl, e.ID, e.Spec, e.Task.Type(), sprintBytes(e.Task.Payload()), e.Opts,
				nextEnqueue(e.Next), prevEnqueue(e.Prev))
		}
	}
	printTable(cols, printRows)
}

// Returns a string describing when the next enqueue will happen.
func nextEnqueue(nextEnqueueAt time.Time) string {
	d := nextEnqueueAt.Sub(time.Now()).Round(time.Second)
	if d < 0 {
		return "Now"
	}
	return fmt.Sprintf("In %v", d)
}

// Returns a string describing when the previous enqueue was.
func prevEnqueue(prevEnqueuedAt time.Time) string {
	if prevEnqueuedAt.IsZero() {
		return "N/A"
	}
	return fmt.Sprintf("%v ago", time.Since(prevEnqueuedAt).Round(time.Second))
}

func cronHistory(cmd *cobra.Command, args []string) {
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
	inspector := createInspector()
	for i, entryID := range args {
		if i > 0 {
			fmt.Printf("\n%s\n", separator)
		}
		fmt.Println()

		fmt.Printf("Entry: %s\n\n", entryID)

		events, err := inspector.ListSchedulerEnqueueEvents(
			entryID, asynq.PageSize(pageSize), asynq.Page(pageNum))
		if err != nil {
			fmt.Printf("error: %v\n", err)
			continue
		}
		if len(events) == 0 {
			fmt.Printf("No scheduler enqueue events found for entry: %s\n", entryID)
			continue
		}

		cols := []string{"TaskID", "EnqueuedAt"}
		printRows := func(w io.Writer, tmpl string) {
			for _, e := range events {
				fmt.Fprintf(w, tmpl, e.TaskID, e.EnqueuedAt)
			}
		}
		printTable(cols, printRows)
	}
}
