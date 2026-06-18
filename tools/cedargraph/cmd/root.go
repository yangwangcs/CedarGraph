package cmd

import (
	"github.com/spf13/cobra"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "cedargraph",
	Short: "CedarGraph distributed graph database CLI",
	Long: `CedarGraph CLI — cluster management and query client.

  cedargraph start    Start CedarGraph cluster
  cedargraph stop     Stop CedarGraph cluster
  cedargraph status   Show cluster status
  cedargraph query    Execute Cypher query
  cedargraph shell    Interactive Cypher shell`,
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "config/cedar.yaml", "Config file path")
}
