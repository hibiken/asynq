package cmd

import (
	"fmt"
	"os"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/spf13/cobra"
)

var enqallValidArgs = []string{"scheduled", "retry", "dead"}

// enqallCmd represents the enqall command
var enqallCmd = &cobra.Command{
	Use:   "enqall [queue name]",
	Short: "Enqueues all tasks from the specified queue",
	Long: `Enqall (asynqmon enqall) will enqueue all tasks from the specified queue.

The argument should be one of "scheduled", "retry", or "dead".

The tasks enqueued by this command will be processed as soon as it
gets dequeued by a processor.

Example: asynqmon enqall dead -> Enqueues all tasks from the dead queue`,
	ValidArgs: enqallValidArgs,
	Args:      cobra.ExactValidArgs(1),
	Run:       enqall,
}

func init() {
	rootCmd.AddCommand(enqallCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// enqallCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// enqallCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func enqall(cmd *cobra.Command, args []string) {
	c := redis.NewClient(&redis.Options{
		Addr: uri,
		DB:   db,
	})
	r := rdb.NewRDB(c)
	var n int64
	var err error
	switch args[0] {
	case "scheduled":
		n, err = r.EnqueueAllScheduledTasks()
	case "retry":
		n, err = r.EnqueueAllRetryTasks()
	case "dead":
		n, err = r.EnqueueAllDeadTasks()
	default:
		fmt.Printf("error: `asynqmon enqall <queue>` only accepts %v as the argument.\n", enqallValidArgs)
		os.Exit(1)
	}
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Enqueued %d tasks from %q queue\n", n, args[0])
}
