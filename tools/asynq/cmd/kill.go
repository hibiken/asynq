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

// killCmd represents the kill command
var killCmd = &cobra.Command{
	Use:   "kill [task key]",
	Short: "Kills a task given an identifier",
	Long: `Kill (asynq kill) will put a task in dead state given an identifier.

The command takes one argument which specifies the task to kill.
The task should be in either scheduled or retry state.
Identifier for a task should be obtained by running "asynq ls" command.

Example: asynq kill r:1575732274:bnogo8gt6toe23vhef0g`,
	Args: cobra.ExactArgs(1),
	Run:  kill,
}

func init() {
	rootCmd.AddCommand(killCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// killCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// killCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func kill(cmd *cobra.Command, args []string) {
	i := asynq.NewInspector(asynq.RedisClientOpt{
		Addr:     viper.GetString("uri"),
		DB:       viper.GetInt("db"),
		Password: viper.GetString("password"),
	})
	err := i.KillTaskByKey(args[0])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Successfully killed %v\n", args[0])

}
