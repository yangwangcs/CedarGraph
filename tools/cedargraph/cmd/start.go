package cmd

import (
	"fmt"
	"time"

	"github.com/cedar-graph/cedargraph-cli/internal/cluster"
	"github.com/spf13/cobra"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start CedarGraph cluster",
	Long:  "Start MetaD, StorageD, GraphD in order and wait for health checks",
	RunE: func(cmd *cobra.Command, args []string) error {
		config, err := cluster.LoadConfig(cfgFile)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}

		mgr := cluster.NewManager(config, ".")

		fmt.Println("\033[1mStarting CedarGraph cluster...\033[0m")
		fmt.Println()

		// Check if already running
		if mgr.IsPortOpen(config.MetaD.Port) {
			fmt.Printf("  \033[33m⚠ MetaD already running on :%d\033[0m\n", config.MetaD.Port)
		} else {
			meta, err := mgr.StartMetaD()
			if err != nil {
				return fmt.Errorf("start metad: %w", err)
			}
			fmt.Printf("  \033[32m●\033[0m MetaD    │ PID %-6d │ %s:%d │ starting...\n",
				meta.PID, meta.Address, meta.Port)

			if !mgr.WaitForPort(config.MetaD.Port, 10*time.Second) {
				return fmt.Errorf("metad did not become ready in time")
			}
			fmt.Printf("  \033[32m●\033[0m MetaD    │ PID %-6d │ %s:%d │ \033[32mready\033[0m\n",
				meta.PID, meta.Address, meta.Port)
		}

		time.Sleep(1 * time.Second) // Leader election

		if mgr.IsPortOpen(config.StorageD.Port) {
			fmt.Printf("  \033[33m⚠ StorageD already running on :%d\033[0m\n", config.StorageD.Port)
		} else {
			storage, err := mgr.StartStorageD()
			if err != nil {
				return fmt.Errorf("start storaged: %w", err)
			}
			fmt.Printf("  \033[32m●\033[0m StorageD │ PID %-6d │ %s:%d │ starting...\n",
				storage.PID, storage.Address, storage.Port)

			if !mgr.WaitForPort(config.StorageD.Port, 10*time.Second) {
				return fmt.Errorf("storaged did not become ready in time")
			}
			fmt.Printf("  \033[32m●\033[0m StorageD │ PID %-6d │ %s:%d │ \033[32mready\033[0m\n",
				storage.PID, storage.Address, storage.Port)
		}

		if mgr.IsPortOpen(config.GraphD.Port) {
			fmt.Printf("  \033[33m⚠ GraphD already running on :%d\033[0m\n", config.GraphD.Port)
		} else {
			graph, err := mgr.StartGraphD()
			if err != nil {
				return fmt.Errorf("start graphd: %w", err)
			}
			fmt.Printf("  \033[32m●\033[0m GraphD   │ PID %-6d │ %s:%d │ starting...\n",
				graph.PID, graph.Address, graph.Port)

			if !mgr.WaitForPort(config.GraphD.Port, 10*time.Second) {
				return fmt.Errorf("graphd did not become ready in time")
			}
			fmt.Printf("  \033[32m●\033[0m GraphD   │ PID %-6d │ %s:%d │ \033[32mready\033[0m\n",
				graph.PID, graph.Address, graph.Port)
		}

		fmt.Println()
		fmt.Println("\033[32mCluster ready.\033[0m Use 'cedargraph status' to check health.")
		fmt.Printf("Connect: cedargraph shell --graphd %s:%d\n", config.GraphD.BindAddr, config.GraphD.Port)
		fmt.Println()
		return nil
	},
}

func init() {
	rootCmd.AddCommand(startCmd)
}
