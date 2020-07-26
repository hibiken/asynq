// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"os"

	"github.com/hibiken/asynq"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var delallValidArgs = []string{"scheduled", "retry", "dead"}

// delallCmd represents the delall command
var delallCmd = &cobra.Command{
	Use:   "delall [state]",
	Short: "Deletes all tasks in the specified state",
	Long: `Delall (asynq delall) will delete all tasks in the specified state.

The argument should be one of "scheduled", "retry", or "dead".

Example: asynq delall dead -> Deletes all dead tasks`,
	ValidArgs: delallValidArgs,
	Args:      cobra.ExactValidArgs(1),
	Run:       delall,
}

func init() {
	rootCmd.AddCommand(delallCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// delallCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// delallCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func delall(cmd *cobra.Command, args []string) {
	i := asynq.NewInspector(asynq.RedisClientOpt{
		Addr:     viper.GetString("uri"),
		DB:       viper.GetInt("db"),
		Password: viper.GetString("password"),
	})
	var (
		n   int
		err error
	)
	switch args[0] {
	case "scheduled":
		n, err = i.DeleteAllScheduledTasks()
	case "retry":
		n, err = i.DeleteAllRetryTasks()
	case "dead":
		n, err = i.DeleteAllDeadTasks()
	default:
		fmt.Printf("error: `asynq delall [state]` only accepts %v as the argument.\n", delallValidArgs)
		os.Exit(1)
	}
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Deleted all %d tasks in %q state\n", n, args[0])
}
