// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"io"

	"github.com/MakeNowJust/heredoc/v2"
	"github.com/fatih/color"
	"github.com/hibiken/asynq"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/spf13/cobra"
)

const separator = "================================================="

func init() {
	rootCmd.AddCommand(queueCmd)
	queueCmd.AddCommand(queueListCmd)
	queueCmd.AddCommand(queueInspectCmd)
	queueCmd.AddCommand(queueHistoryCmd)
	queueHistoryCmd.Flags().IntP("days", "x", 10, "show data from last x days")

	queueCmd.AddCommand(queuePauseCmd)
	queueCmd.AddCommand(queueUnpauseCmd)
	queueCmd.AddCommand(queueRemoveCmd)
	queueRemoveCmd.Flags().BoolP("force", "f", false, "remove the queue regardless of its size")
}

var queueCmd = &cobra.Command{
	Use:   "queue <command> [flags]",
	Short: "Manage queues",
	Example: heredoc.Doc(`
	  $ asynq queue ls
	  $ asynq queue inspect myqueue
	  $ asynq queue pause myqueue`),
}

var queueListCmd = &cobra.Command{
	Use:     "list",
	Short:   "List queues",
	Aliases: []string{"ls"},
	RunE:    queueList,
}

var queueInspectCmd = &cobra.Command{
	Use:   "inspect <queue> [<queue>...]",
	Short: "Display detailed information on one or more queues",
	Args: cobra.MinimumNArgs(1),
	RunE: queueInspect,
	Example: heredoc.Doc(`
		$ asynq queue inspect myqueue
		$ asynq queue inspect queue1 queue2 queue3`),
}

var queueHistoryCmd = &cobra.Command{
	Use:   "history <queue> [<queue>...]",
	Short: "Display historical aggregate data from one or more queues",
	Args: cobra.MinimumNArgs(1),
	RunE: queueHistory,
	Example: heredoc.Doc(`
		$ asynq queue history myqueue
		$ asynq queue history queue1 queue2 queue3
		$ asynq queue history myqueue --days=90`),
}

var queuePauseCmd = &cobra.Command{
	Use:   "pause <queue> [<queue>...]",
	Short: "Pause one or more queues",
	Args:  cobra.MinimumNArgs(1),
	RunE:  queuePause,
	Example: heredoc.Doc(`
		$ asynq queue pause myqueue
		$ asynq queue pause queue1 queue2 queue3`),
}

var queueUnpauseCmd = &cobra.Command{
	Use:     "resume <queue> [<queue>...]",
	Short:   "Resume (unpause) one or more queues",
	Args:    cobra.MinimumNArgs(1),
	Aliases: []string{"unpause"},
	RunE:    queueUnpause,
	Example: heredoc.Doc(`
		$ asynq queue resume myqueue
		$ asynq queue resume queue1 queue2 queue3`),
}

var queueRemoveCmd = &cobra.Command{
	Use:     "remove <queue> [<queue>...]",
	Short:   "Remove one or more queues",
	Aliases: []string{"rm", "delete"},
	Args:    cobra.MinimumNArgs(1),
	RunE:    queueRemove,
	Example: heredoc.Doc(`
		$ asynq queue rm myqueue
		$ asynq queue rm queue1 queue2 queue3
		$ asynq queue rm myqueue --force`),
}

func queueList(cmd *cobra.Command, args []string) error {
	type queueInfo struct {
		name    string
		keyslot int64
		nodes   []*asynq.ClusterNode
	}
	inspector := createInspector()
	queues, err := inspector.Queues()
	if err != nil {
		return fmt.Errorf("could not fetch list of queues: %v", err)
	}
	var qs []*queueInfo
	for _, qname := range queues {
		q := queueInfo{name: qname}
		if useRedisCluster {
			keyslot, err := inspector.ClusterKeySlot(qname)
			if err != nil {
				fmt.Printf("error: could not get cluster keyslot for %q\n", qname)
				continue
			}
			q.keyslot = keyslot
			nodes, err := inspector.ClusterNodes(qname)
			if err != nil {
				fmt.Printf("error: could not get cluster nodes for %q\n", qname)
				continue
			}
			q.nodes = nodes
		}
		qs = append(qs, &q)
	}
	if useRedisCluster {
		printTable(
			[]string{"Queue", "Cluster KeySlot", "Cluster Nodes"},
			func(w io.Writer, tmpl string) {
				for _, q := range qs {
					fmt.Fprintf(w, tmpl, q.name, q.keyslot, q.nodes)
				}
			},
		)
	} else {
		for _, q := range qs {
			fmt.Println(q.name)
		}
	}
	return nil
}

func queueInspect(cmd *cobra.Command, args []string) error {
	inspector := createInspector()
	for i, qname := range args {
		if i > 0 {
			fmt.Printf("\n%s\n\n", separator)
		}
		info, err := inspector.GetQueueInfo(qname)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			continue
		}
		printQueueInfo(info)
	}
	return nil
}

func printQueueInfo(info *asynq.QueueInfo) {
	bold := color.New(color.Bold)
	bold.Println("Queue Info")
	fmt.Printf("Name:   %s\n", info.Queue)
	fmt.Printf("Size:   %d\n", info.Size)
	fmt.Printf("Groups: %d\n", info.Groups)
	fmt.Printf("Paused: %t\n\n", info.Paused)
	bold.Println("Task Count by State")
	printTable(
		[]string{"active", "pending", "aggregating", "scheduled", "retry", "archived", "completed"},
		func(w io.Writer, tmpl string) {
			fmt.Fprintf(w, tmpl, info.Active, info.Pending, info.Aggregating, info.Scheduled, info.Retry, info.Archived, info.Completed)
		},
	)
	fmt.Println()
	bold.Printf("Daily Stats %s UTC\n", info.Timestamp.UTC().Format("2006-01-02"))
	printTable(
		[]string{"processed", "failed", "error rate"},
		func(w io.Writer, tmpl string) {
			var errRate string
			if info.Processed == 0 {
				errRate = "N/A"
			} else {
				errRate = fmt.Sprintf("%.2f%%", float64(info.Failed)/float64(info.Processed)*100)
			}
			fmt.Fprintf(w, tmpl, info.Processed, info.Failed, errRate)
		},
	)
}

func queueHistory(cmd *cobra.Command, args []string) error {
	days, err := cmd.Flags().GetInt("days")
	if err != nil {
		return err
	}
	inspector := createInspector()
	for i, qname := range args {
		if i > 0 {
			fmt.Printf("\n%s\n\n", separator)
		}
		fmt.Printf("Queue: %s\n\n", qname)
		stats, err := inspector.History(qname, days)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			continue
		}
		printDailyStats(stats)
	}
	return nil
}

func printDailyStats(stats []*asynq.DailyStats) {
	printTable(
		[]string{"date (UTC)", "processed", "failed", "error rate"},
		func(w io.Writer, tmpl string) {
			for _, s := range stats {
				var errRate string
				if s.Processed == 0 {
					errRate = "N/A"
				} else {
					errRate = fmt.Sprintf("%.2f%%", float64(s.Failed)/float64(s.Processed)*100)
				}
				fmt.Fprintf(w, tmpl, s.Date.Format("2006-01-02"), s.Processed, s.Failed, errRate)
			}
		},
	)
}

func queuePause(cmd *cobra.Command, args []string) error {
	inspector := createInspector()
	var firstErr error
	for _, qname := range args {
		err := inspector.PauseQueue(qname)
		if err != nil {
			fmt.Println(err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		fmt.Printf("Successfully paused queue %q\n", qname)
	}
	return firstErr
}

func queueUnpause(cmd *cobra.Command, args []string) error {
	inspector := createInspector()
	var firstErr error
	for _, qname := range args {
		err := inspector.UnpauseQueue(qname)
		if err != nil {
			fmt.Println(err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		fmt.Printf("Successfully unpaused queue %q\n", qname)
	}
	return firstErr
}

func queueRemove(cmd *cobra.Command, args []string) error {
	// TODO: Use inspector once RemoveQueue become public API.
	force, err := cmd.Flags().GetBool("force")
	if err != nil {
		return err
	}

	r := createRDB()
	var firstErr error
	for _, qname := range args {
		err = r.RemoveQueue(qname, force)
		if err != nil {
			if errors.IsQueueNotEmpty(err) {
				fmt.Printf("error: %v\nIf you are sure you want to delete it, run 'asynq queue rm --force %s'\n", err, qname)
			} else {
				fmt.Printf("error: %v\n", err)
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		fmt.Printf("Successfully removed queue %q\n", qname)
	}
	return firstErr
}
