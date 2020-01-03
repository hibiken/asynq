// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

var cfgFile string

// Flags
var uri string
var db int

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "asynqmon",
	Short: "A monitoring tool for asynq queues",
	Long: `Asynqmon is a CLI tool to inspect and monitor queues managed by asynq package.

Asynqmon has a few commands to query and mutate the current state of the queues.

Monitoring commands such as "stats" and "ls" can be used in conjunction with the
"watch" command to continuously run the command at a certain interval.
	
Example: watch -n 5 asynqmon stats`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.asynqmon.yaml)")
	rootCmd.PersistentFlags().StringVarP(&uri, "uri", "u", "127.0.0.1:6379", "Redis server URI")
	rootCmd.PersistentFlags().IntVarP(&db, "db", "n", 0, "Redis database number (default is 0)")
}

// initConfig reads in config file and ENV variables if set.
// TODO(hibiken): Remove this if not necessary.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".asynqmon" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".asynqmon")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
