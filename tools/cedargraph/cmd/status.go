package cmd

import (
	"fmt"

	"github.com/cedar-graph/cedargraph-cli/internal/cluster"
	"github.com/cedar-graph/cedargraph-cli/internal/display"
	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show CedarGraph cluster status",
	Long:  "Query MetaD and display cluster topology, node health, partition info",
	RunE: func(cmd *cobra.Command, args []string) error {
		config, err := cluster.LoadConfig(cfgFile)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}

		mgr := cluster.NewManager(config, ".")

		fmt.Println()
		fmt.Println("\033[1mCedarGraph Cluster Status\033[0m")

		roles := map[string]string{
			"MetaD":    "leader",
			"StorageD": "storage",
			"GraphD":   "query",
		}

		nodeInfos := []cluster.NodeInfo{
			mgr.GetNodeStatus("MetaD", config.MetaD),
			mgr.GetNodeStatus("StorageD", config.StorageD),
			mgr.GetNodeStatus("GraphD", config.GraphD),
		}

		var nodes []display.NodeStatus
		for _, ni := range nodeInfos {
			nodes = append(nodes, display.NodeStatus{
				Name:    ni.Name,
				Address: ni.Address,
				Port:    ni.Port,
				Role:    roles[ni.Name],
				Status:  ni.Status,
				PID:     ni.PID,
			})
		}

		display.PrintClusterStatus(nodes)
		fmt.Println()
		return nil
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)
}
