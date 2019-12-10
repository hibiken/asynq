package cmd

import (
	"fmt"
	"os"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/spf13/cobra"
)

// enqCmd represents the enq command
var enqCmd = &cobra.Command{
	Use:   "enq",
	Short: "Enqueues a task given an identifier",
	Long: `The enq command enqueues a task given an identifier.

The command takes one argument which specifies the task to enqueue.
The task should be in either scheduled, retry or dead queue.
Identifier for a task should be obtained by running "asynqmon ls" command.

The task enqueued by this command will be processed as soon as the task 
gets dequeued by a processor.

Example: asynqmon enq d:1575732274:b0415aa2-fd33-4b63-87c4-2f1a954ea4bf`,
	Args: cobra.ExactArgs(1),
	Run:  enq,
}

func init() {
	rootCmd.AddCommand(enqCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// enqCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// enqCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func enq(cmd *cobra.Command, args []string) {
	id, score, qtype, err := parseQueryID(args[0])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	r := rdb.NewRDB(redis.NewClient(&redis.Options{
		Addr: uri,
		DB:   db,
	}))
	switch qtype {
	case "s":
		err = r.EnqueueScheduledTask(id.String(), float64(score))
	case "r":
		err = r.EnqueueRetryTask(id.String(), float64(score))
	case "d":
		err = r.EnqueueDeadTask(id.String(), float64(score))
	default:
		fmt.Println("invalid argument")
		os.Exit(1)
	}
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Successfully enqueued %v\n", args[0])
}
