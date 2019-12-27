package cmd

import (
	"fmt"
	"os"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/spf13/cobra"
)

var killallValidArgs = []string{"scheduled", "retry"}

// killallCmd represents the killall command
var killallCmd = &cobra.Command{
	Use:   "killall [queue name]",
	Short: "Sends all tasks to dead queue from the specified queue",
	Long: `Killall (asynqmon killall) will moves all tasks from the specified queue to dead queue.

The argument should be either "scheduled" or "retry".

Example: asynqmon killall retry -> Moves all tasks from retry queue to dead queue`,
	ValidArgs: killallValidArgs,
	Args:      cobra.ExactValidArgs(1),
	Run:       killall,
}

func init() {
	rootCmd.AddCommand(killallCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// killallCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// killallCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func killall(cmd *cobra.Command, args []string) {
	c := redis.NewClient(&redis.Options{
		Addr: uri,
		DB:   db,
	})
	r := rdb.NewRDB(c)
	var n int64
	var err error
	switch args[0] {
	case "scheduled":
		n, err = r.KillAllScheduledTasks()
	case "retry":
		n, err = r.KillAllRetryTasks()
	default:
		fmt.Printf("error: `asynqmon killall [queue name]` only accepts %v as the argument.\n", killallValidArgs)
		os.Exit(1)
	}
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Sent %d tasks to \"dead\" queue from %q queue\n", n, args[0])
}
