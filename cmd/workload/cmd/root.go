package cmd

import "github.com/spf13/cobra"

var (
	// Config flags
	dbHost            string
	dbUser            string
	dbPassword        string
	dbName            string
	seed              int64
	numParents        int
	childrenPerParent int

	rootCmd = &cobra.Command{
		Use:   "workload",
		Short: "MySQL workload generator",
		Long:  `A concurrent workload generator for MySQL databases with support for multiple workers and customizable operations.`,
	}
)

// Execute executes the root command
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// Global flags for database connection
	rootCmd.PersistentFlags().StringVar(&dbHost, "host", "localhost", "Database host")
	rootCmd.PersistentFlags().StringVar(&dbUser, "user", "root", "Database user")
	rootCmd.PersistentFlags().StringVar(&dbPassword, "password", "", "Database password")
	rootCmd.PersistentFlags().StringVar(&dbName, "dbname", "test_db", "Database name")
	rootCmd.PersistentFlags().Int64Var(&seed, "seed", 0, "Random seed (default: current timestamp)")
	rootCmd.PersistentFlags().IntVar(&numParents, "parents", 10, "Number of parent records")
	rootCmd.PersistentFlags().IntVar(&childrenPerParent, "children", 5, "Number of children per parent")

	// Add subcommands
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(runCmd)
}
