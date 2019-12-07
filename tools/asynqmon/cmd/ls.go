package cmd

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/spf13/cobra"
)

var validArgs = []string{"enqueued", "inprogress", "scheduled", "retry", "dead"}

// lsCmd represents the ls command
var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "lists queue contents",
	Long: `The ls command lists all tasks from the given queue in a table format.

The command takes one argument which specifies the queue. The value
for the argument should be one of "enqueued", "inprogress", "scheduled",
"retry", or "dead".

Example: asynqmon ls dead`,
	ValidArgs: validArgs,
	Args:      cobra.ExactValidArgs(1),
	Run:       ls,
}

func init() {
	rootCmd.AddCommand(lsCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// lsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// lsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func ls(cmd *cobra.Command, args []string) {
	c := redis.NewClient(&redis.Options{
		Addr: uri,
		DB:   db,
	})
	r := rdb.NewRDB(c)
	switch args[0] {
	case "enqueued":
		listEnqueued(r)
	case "inprogress":
		listInProgress(r)
	case "scheduled":
		listScheduled(r)
	case "retry":
		listRetry(r)
	case "dead":
		listDead(r)
	default:
		fmt.Printf("error: `asynqmon ls <queue>` only accepts %v as the argument for queue.\n", validArgs)
		return
	}
}

func listEnqueued(r *rdb.RDB) {
	tasks, err := r.ListEnqueued()
	if err != nil {
		log.Fatal(err)
	}
	if len(tasks) == 0 {
		fmt.Println("No enqueued tasks")
		return
	}
	cols := []string{"ID", "Type", "Payload"}
	printRows := func(w io.Writer, tmpl string) {
		for _, t := range tasks {
			fmt.Fprintf(w, tmpl, t.ID, t.Type, t.Payload)
		}
	}
	printTable(cols, printRows)
}

func listInProgress(r *rdb.RDB) {
	tasks, err := r.ListInProgress()
	if err != nil {
		log.Fatal(err)
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
}

func listScheduled(r *rdb.RDB) {
	tasks, err := r.ListScheduled()
	if err != nil {
		log.Fatal(err)
	}
	if len(tasks) == 0 {
		fmt.Println("No scheduled tasks")
		return
	}
	cols := []string{"ID", "Type", "Payload", "Process In"}
	printRows := func(w io.Writer, tmpl string) {
		for _, t := range tasks {
			processIn := fmt.Sprintf("%.0f seconds", t.ProcessAt.Sub(time.Now()).Seconds())
			fmt.Fprintf(w, tmpl, t.ID, t.Type, t.Payload, processIn)
		}
	}
	printTable(cols, printRows)
}

func listRetry(r *rdb.RDB) {
	tasks, err := r.ListRetry()
	if err != nil {
		log.Fatal(err)
	}
	if len(tasks) == 0 {
		fmt.Println("No retry tasks")
		return
	}
	cols := []string{"ID", "Type", "Payload", "Retry In", "Last Error", "Retried", "Max Retry"}
	printRows := func(w io.Writer, tmpl string) {
		for _, t := range tasks {
			retryIn := fmt.Sprintf("%.0f seconds", t.ProcessAt.Sub(time.Now()).Seconds())
			fmt.Fprintf(w, tmpl, t.ID, t.Type, t.Payload, retryIn, t.ErrorMsg, t.Retried, t.Retry)
		}
	}
	printTable(cols, printRows)
}

func listDead(r *rdb.RDB) {
	tasks, err := r.ListDead()
	if err != nil {
		log.Fatal(err)
	}
	if len(tasks) == 0 {
		fmt.Println("No dead tasks")
		return
	}
	cols := []string{"ID", "Type", "Payload", "Last Failed", "Last Error"}
	printRows := func(w io.Writer, tmpl string) {
		for _, t := range tasks {
			fmt.Fprintf(w, tmpl, t.ID, t.Type, t.Payload, t.LastFailedAt, t.ErrorMsg)
		}
	}
	printTable(cols, printRows)
}

func printTable(cols []string, printRows func(w io.Writer, tmpl string)) {
	format := strings.Repeat("%v\t", len(cols)) + "\n"
	tw := new(tabwriter.Writer).Init(os.Stdout, 0, 8, 2, ' ', 0)
	var headers []interface{}
	var seps []interface{}
	for _, name := range cols {
		headers = append(headers, name)
		seps = append(seps, strings.Repeat("-", len(name)))
	}
	fmt.Fprintf(tw, format, headers...)
	fmt.Fprintf(tw, format, seps...)
	printRows(tw, format)
	tw.Flush()
}
