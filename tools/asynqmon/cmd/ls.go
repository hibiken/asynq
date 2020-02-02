// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/rs/xid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var lsValidArgs = []string{"enqueued", "inprogress", "scheduled", "retry", "dead"}

// lsCmd represents the ls command
var lsCmd = &cobra.Command{
	Use:   "ls [state]",
	Short: "Lists tasks in the specified state",
	Long: `Ls (asynqmon ls) will list all tasks in the specified state in a table format.

The command takes one argument which specifies the state of tasks.
The argument value should be one of "enqueued", "inprogress", "scheduled",
"retry", or "dead".

Example:
asynqmon ls dead -> Lists all tasks in dead state

Enqueued tasks requires a queue name after ":"
Example:
asynqmon ls enqueued:default  -> List tasks from default queue
asynqmon ls enqueued:critical -> List tasks from critical queue 
`,
	Args: cobra.ExactValidArgs(1),
	Run:  ls,
}

// Flags
var pageSize uint
var pageNum uint

func init() {
	rootCmd.AddCommand(lsCmd)
	lsCmd.Flags().UintVar(&pageSize, "size", 30, "page size")
	lsCmd.Flags().UintVar(&pageNum, "page", 0, "page number - zero indexed (default 0)")
}

func ls(cmd *cobra.Command, args []string) {
	c := redis.NewClient(&redis.Options{
		Addr:     viper.GetString("uri"),
		DB:       viper.GetInt("db"),
		Password: viper.GetString("password"),
	})
	r := rdb.NewRDB(c)
	parts := strings.Split(args[0], ":")
	switch parts[0] {
	case "enqueued":
		if len(parts) != 2 {
			fmt.Printf("error: Missing queue name\n`asynqmon ls enqueued:[queue name]`\n")
			os.Exit(1)
		}
		listEnqueued(r, parts[1])
	case "inprogress":
		listInProgress(r)
	case "scheduled":
		listScheduled(r)
	case "retry":
		listRetry(r)
	case "dead":
		listDead(r)
	default:
		fmt.Printf("error: `asynqmon ls [state]`\nonly accepts %v as the argument.\n", lsValidArgs)
		os.Exit(1)
	}
}

// queryID returns an identifier used for "enq" command.
// score is the zset score and queryType should be one
// of "s", "r" or "d" (scheduled, retry, dead respectively).
func queryID(id xid.ID, score int64, qtype string) string {
	const format = "%v:%v:%v"
	return fmt.Sprintf(format, qtype, score, id)
}

// parseQueryID is a reverse operation of queryID function.
// It takes a queryID and return each part of id with proper
// type if valid, otherwise it reports an error.
func parseQueryID(queryID string) (id xid.ID, score int64, qtype string, err error) {
	parts := strings.Split(queryID, ":")
	if len(parts) != 3 {
		return xid.NilID(), 0, "", fmt.Errorf("invalid id")
	}
	id, err = xid.FromString(parts[2])
	if err != nil {
		return xid.NilID(), 0, "", fmt.Errorf("invalid id")
	}
	score, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return xid.NilID(), 0, "", fmt.Errorf("invalid id")
	}
	qtype = parts[0]
	if len(qtype) != 1 || !strings.Contains("srd", qtype) {
		return xid.NilID(), 0, "", fmt.Errorf("invalid id")
	}
	return id, score, qtype, nil
}

func listEnqueued(r *rdb.RDB, qname string) {
	tasks, err := r.ListEnqueued(qname, rdb.Pagination{Size: pageSize, Page: pageNum})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Printf("No enqueued tasks in %q queue\n", qname)
		return
	}
	cols := []string{"ID", "Type", "Payload", "Queue"}
	printRows := func(w io.Writer, tmpl string) {
		for _, t := range tasks {
			fmt.Fprintf(w, tmpl, t.ID, t.Type, t.Payload, t.Queue)
		}
	}
	printTable(cols, printRows)
	fmt.Printf("\nShowing %d tasks from page %d\n", len(tasks), pageNum)
}

func listInProgress(r *rdb.RDB) {
	tasks, err := r.ListInProgress(rdb.Pagination{Size: pageSize, Page: pageNum})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Println("No in-progress tasks")
		return
	}
	cols := []string{"ID", "Type", "Payload"}
	printRows := func(w io.Writer, tmpl string) {
		for _, t := range tasks {
			fmt.Fprintf(w, tmpl, t.ID, t.Type, t.Payload)
		}
	}
	printTable(cols, printRows)
	fmt.Printf("\nShowing %d tasks from page %d\n", len(tasks), pageNum)
}

func listScheduled(r *rdb.RDB) {
	tasks, err := r.ListScheduled(rdb.Pagination{Size: pageSize, Page: pageNum})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Println("No scheduled tasks")
		return
	}
	cols := []string{"ID", "Type", "Payload", "Process In", "Queue"}
	printRows := func(w io.Writer, tmpl string) {
		for _, t := range tasks {
			processIn := fmt.Sprintf("%.0f seconds", t.ProcessAt.Sub(time.Now()).Seconds())
			fmt.Fprintf(w, tmpl, queryID(t.ID, t.Score, "s"), t.Type, t.Payload, processIn, t.Queue)
		}
	}
	printTable(cols, printRows)
	fmt.Printf("\nShowing %d tasks from page %d\n", len(tasks), pageNum)
}

func listRetry(r *rdb.RDB) {
	tasks, err := r.ListRetry(rdb.Pagination{Size: pageSize, Page: pageNum})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Println("No retry tasks")
		return
	}
	cols := []string{"ID", "Type", "Payload", "Retry In", "Last Error", "Retried", "Max Retry", "Queue"}
	printRows := func(w io.Writer, tmpl string) {
		for _, t := range tasks {
			retryIn := fmt.Sprintf("%.0f seconds", t.ProcessAt.Sub(time.Now()).Seconds())
			fmt.Fprintf(w, tmpl, queryID(t.ID, t.Score, "r"), t.Type, t.Payload, retryIn, t.ErrorMsg, t.Retried, t.Retry, t.Queue)
		}
	}
	printTable(cols, printRows)
	fmt.Printf("\nShowing %d tasks from page %d\n", len(tasks), pageNum)
}

func listDead(r *rdb.RDB) {
	tasks, err := r.ListDead(rdb.Pagination{Size: pageSize, Page: pageNum})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(tasks) == 0 {
		fmt.Println("No dead tasks")
		return
	}
	cols := []string{"ID", "Type", "Payload", "Last Failed", "Last Error", "Queue"}
	printRows := func(w io.Writer, tmpl string) {
		for _, t := range tasks {
			fmt.Fprintf(w, tmpl, queryID(t.ID, t.Score, "d"), t.Type, t.Payload, t.LastFailedAt, t.ErrorMsg, t.Queue)
		}
	}
	printTable(cols, printRows)
	fmt.Printf("\nShowing %d tasks from page %d\n", len(tasks), pageNum)
}
