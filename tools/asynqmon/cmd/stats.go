package cmd

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/spf13/cobra"
)

// statsCmd represents the stats command
var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Shows current state of the queues",
	Long: `Stats (aysnqmon stats) will show the number of tasks in each queue at that instant.

To monitor the queues continuously, it's recommended that you run this
command in conjunction with the watch command.

Example: watch -n 5 asynqmon stats`,
	Args: cobra.NoArgs,
	Run:  stats,
}

func init() {
	rootCmd.AddCommand(statsCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// statsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// statsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func stats(cmd *cobra.Command, args []string) {
	c := redis.NewClient(&redis.Options{
		Addr: uri,
		DB:   db,
	})
	r := rdb.NewRDB(c)

	stats, err := r.CurrentStats()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	printStats(stats)
	fmt.Println()
}

func printStats(s *rdb.Stats) {
	format := strings.Repeat("%v\t", 5) + "\n"
	tw := new(tabwriter.Writer).Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(tw, format, "Enqueued", "InProgress", "Scheduled", "Retry", "Dead")
	fmt.Fprintf(tw, format, "--------", "----------", "---------", "-----", "----")
	fmt.Fprintf(tw, format, s.Enqueued, s.InProgress, s.Scheduled, s.Retry, s.Dead)
	tw.Flush()
}
