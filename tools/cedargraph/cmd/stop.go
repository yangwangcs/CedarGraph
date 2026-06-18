package cmd

import (
	"fmt"

	"github.com/cedar-graph/cedargraph-cli/internal/cluster"
	"github.com/spf13/cobra"
)

var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop CedarGraph cluster",
	Long:  "Send SIGTERM to all CedarGraph nodes",
	RunE: func(cmd *cobra.Command, args []string) error {
		config, err := cluster.LoadConfig(cfgFile)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}

		mgr := cluster.NewManager(config, ".")

		fmt.Println("Stopping CedarGraph cluster...")
		errs := mgr.StopAll()
		if len(errs) > 0 {
			for _, e := range errs {
				fmt.Printf("  \033[31m✗ %v\033[0m\n", e)
			}
			return fmt.Errorf("failed to stop %d node(s)", len(errs))
		}

		fmt.Println("\033[32mCluster stopped.\033[0m")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(stopCmd)
}
