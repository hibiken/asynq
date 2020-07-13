// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var lsValidArgs = []string{"enqueued", "inprogress", "scheduled", "retry", "dead"}

// lsCmd represents the ls command
var lsCmd = &cobra.Command{
	Use:   "ls [state]",
	Short: "Lists tasks in the specified state",
	Long: `Ls (asynq ls) will list all tasks in the specified state in a table format.

The command takes one argument which specifies the state of tasks.
The argument value should be one of "enqueued", "inprogress", "scheduled",
"retry", or "dead".

Example:
asynq ls dead -> Lists all tasks in dead state

Enqueued tasks requires a queue name after ":"
Example:
asynq ls enqueued:default  -> List tasks from default queue
asynq ls enqueued:critical -> List tasks from critical queue 
`,
	Args: cobra.ExactValidArgs(1),
	Run:  ls,
}

// Flags
var pageSize int
var pageNum int

func init() {
	rootCmd.AddCommand(lsCmd)
	lsCmd.Flags().IntVar(&pageSize, "size", 30, "page size")
	lsCmd.Flags().IntVar(&pageNum, "page", 0, "page number - zero indexed (default 0)")
}

func ls(cmd *cobra.Command, args []string) {
	if pageSize < 0 {
		fmt.Println("page size cannot be negative.")
		os.Exit(1)
	}
	if pageNum < 0 {
		fmt.Println("page number cannot be negative.")
		os.Exit(1)
	}
	i := asynq.NewInspector(asynq.RedisClientOpt{
		Addr:     viper.GetString("uri"),
		DB:       viper.GetInt("db"),
		Password: viper.GetString("password"),
	})
	parts := strings.Split(args[0], ":")
	switch parts[0] {
	case "enqueued":
		if len(parts) != 2 {
			fmt.Printf("error: Missing queue name\n`asynq ls enqueued:[queue name]`\n")
			os.Exit(1)
		}
		listEnqueued(i, parts[1])
	case "inprogress":
		listInProgress(i)
	case "scheduled":
		listScheduled(i)
	case "retry":
		listRetry(i)
	case "dead":
		listDead(i)
	default:
		fmt.Printf("error: `asynq ls [state]`\nonly accepts %v as the argument.\n", lsValidArgs)
		os.Exit(1)
	}
}

func listEnqueued(i *asynq.Inspector, qname string) {
	tasks, err := i.ListEnqueuedTasks(qname, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Printf("No enqueued tasks in %q queue\n", qname)
		return
	}
	cols := []string{"ID", "Type", "Payload", "Queue"}
	printTable(cols, func(w io.Writer, tmpl string) {
		for _, t := range tasks {
			fmt.Fprintf(w, tmpl, t.ID, t.Type, t.Payload, t.Queue)
		}
	})
	fmt.Printf("\nShowing %d tasks from page %d\n", len(tasks), pageNum)
}

func listInProgress(i *asynq.Inspector) {
	tasks, err := i.ListInProgressTasks(asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Println("No in-progress tasks")
		return
	}
	cols := []string{"ID", "Type", "Payload"}
	printTable(cols, func(w io.Writer, tmpl string) {
		for _, t := range tasks {
			fmt.Fprintf(w, tmpl, t.ID, t.Type, t.Payload)
		}
	})
	fmt.Printf("\nShowing %d tasks from page %d\n", len(tasks), pageNum)
}

func listScheduled(i *asynq.Inspector) {
	tasks, err := i.ListScheduledTasks(asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Println("No scheduled tasks")
		return
	}
	cols := []string{"Key", "Type", "Payload", "Process In", "Queue"}
	printTable(cols, func(w io.Writer, tmpl string) {
		for _, t := range tasks {
			processIn := fmt.Sprintf("%.0f seconds",
				t.NextEnqueueAt.Sub(time.Now()).Seconds())
			fmt.Fprintf(w, tmpl, t.Key(), t.Type, t.Payload, processIn, t.Queue)
		}
	})
	fmt.Printf("\nShowing %d tasks from page %d\n", len(tasks), pageNum)
}

func listRetry(i *asynq.Inspector) {
	tasks, err := i.ListRetryTasks(asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Println("No retry tasks")
		return
	}
	cols := []string{"Key", "Type", "Payload", "Next Retry", "Last Error", "Retried", "Max Retry", "Queue"}
	printTable(cols, func(w io.Writer, tmpl string) {
		for _, t := range tasks {
			var nextRetry string
			if d := t.NextEnqueueAt.Sub(time.Now()); d > 0 {
				nextRetry = fmt.Sprintf("in %v", d.Round(time.Second))
			} else {
				nextRetry = "right now"
			}
			fmt.Fprintf(w, tmpl, t.Key(), t.Type, t.Payload, nextRetry, t.ErrorMsg, t.Retried, t.MaxRetry, t.Queue)
		}
	})
	fmt.Printf("\nShowing %d tasks from page %d\n", len(tasks), pageNum)
}

func listDead(i *asynq.Inspector) {
	tasks, err := i.ListDeadTasks(asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Println("No dead tasks")
		return
	}
	cols := []string{"Key", "Type", "Payload", "Last Failed", "Last Error", "Queue"}
	printTable(cols, func(w io.Writer, tmpl string) {
		for _, t := range tasks {
			fmt.Fprintf(w, tmpl, t.Key(), t.Type, t.Payload, t.LastFailedAt, t.ErrorMsg, t.Queue)
		}
	})
	fmt.Printf("\nShowing %d tasks from page %d\n", len(tasks), pageNum)
}
