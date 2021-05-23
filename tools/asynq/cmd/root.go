// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"crypto/tls"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/spf13/cobra"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

var cfgFile string

// Global flag variables
var (
	uri      string
	db       int
	password string

	useRedisCluster bool
	clusterAddrs    string
	tlsServerName   string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:     "asynq",
	Short:   "A monitoring tool for asynq queues",
	Long:    `Asynq is a montoring CLI to inspect tasks and queues managed by asynq.`,
	Version: base.Version,
}

var versionOutput = fmt.Sprintf("asynq version %s\n", base.Version)

var versionCmd = &cobra.Command{
	Use:    "version",
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Print(versionOutput)
	},
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

	rootCmd.AddCommand(versionCmd)
	rootCmd.SetVersionTemplate(versionOutput)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file to set flag defaut values (default is $HOME/.asynq.yaml)")
	rootCmd.PersistentFlags().StringVarP(&uri, "uri", "u", "127.0.0.1:6379", "redis server URI")
	rootCmd.PersistentFlags().IntVarP(&db, "db", "n", 0, "redis database number (default is 0)")
	rootCmd.PersistentFlags().StringVarP(&password, "password", "p", "", "password to use when connecting to redis server")
	rootCmd.PersistentFlags().BoolVar(&useRedisCluster, "cluster", false, "connect to redis cluster")
	rootCmd.PersistentFlags().StringVar(&clusterAddrs, "cluster_addrs",
		"127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005",
		"list of comma-separated redis server addresses")
	rootCmd.PersistentFlags().StringVar(&tlsServerName, "tls_server",
		"", "server name for TLS validation")
	// Bind flags with config.
	viper.BindPFlag("uri", rootCmd.PersistentFlags().Lookup("uri"))
	viper.BindPFlag("db", rootCmd.PersistentFlags().Lookup("db"))
	viper.BindPFlag("password", rootCmd.PersistentFlags().Lookup("password"))
	viper.BindPFlag("cluster", rootCmd.PersistentFlags().Lookup("cluster"))
	viper.BindPFlag("cluster_addrs", rootCmd.PersistentFlags().Lookup("cluster_addrs"))
	viper.BindPFlag("tls_server", rootCmd.PersistentFlags().Lookup("tls_server"))
}

// initConfig reads in config file and ENV variables if set.
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

		// Search config in home directory with name ".asynq" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".asynq")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

// createRDB creates a RDB instance using flag values and returns it.
func createRDB() *rdb.RDB {
	var c redis.UniversalClient
	if useRedisCluster {
		addrs := strings.Split(viper.GetString("cluster_addrs"), ",")
		c = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:     addrs,
			Password:  viper.GetString("password"),
			TLSConfig: getTLSConfig(),
		})
	} else {
		c = redis.NewClient(&redis.Options{
			Addr:      viper.GetString("uri"),
			DB:        viper.GetInt("db"),
			Password:  viper.GetString("password"),
			TLSConfig: getTLSConfig(),
		})
	}
	return rdb.NewRDB(c)
}

// createRDB creates a Inspector instance using flag values and returns it.
func createInspector() *asynq.Inspector {
	var connOpt asynq.RedisConnOpt
	if useRedisCluster {
		addrs := strings.Split(viper.GetString("cluster_addrs"), ",")
		connOpt = asynq.RedisClusterClientOpt{
			Addrs:     addrs,
			Password:  viper.GetString("password"),
			TLSConfig: getTLSConfig(),
		}
	} else {
		connOpt = asynq.RedisClientOpt{
			Addr:      viper.GetString("uri"),
			DB:        viper.GetInt("db"),
			Password:  viper.GetString("password"),
			TLSConfig: getTLSConfig(),
		}
	}
	return asynq.NewInspector(connOpt)
}

func getTLSConfig() *tls.Config {
	tlsServer := viper.GetString("tls_server")
	if tlsServer == "" {
		return nil
	}
	return &tls.Config{ServerName: tlsServer}
}

// printTable is a helper function to print data in table format.
//
// cols is a list of headers and printRow specifies how to print rows.
//
// Example:
// type User struct {
//     Name string
//     Addr string
//     Age  int
// }
// data := []*User{{"user1", "addr1", 24}, {"user2", "addr2", 42}, ...}
// cols := []string{"Name", "Addr", "Age"}
// printRows := func(w io.Writer, tmpl string) {
//     for _, u := range data {
//         fmt.Fprintf(w, tmpl, u.Name, u.Addr, u.Age)
//     }
// }
// printTable(cols, printRows)
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
