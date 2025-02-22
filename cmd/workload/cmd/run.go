// cmd/workload/cmd/run.go

package cmd

import (
	"fmt"
	"log"
	"time"

	"github.com/shailendra-patel/mysql-workload/internal/database"
	"github.com/shailendra-patel/mysql-workload/internal/workload"
	"github.com/spf13/cobra"
)

var (
	numWorkers int

	runCmd = &cobra.Command{
		Use:   "run",
		Short: "Run the workload generator",
		Long:  `Starts the workload generator with the specified number of concurrent workers.`,
		RunE:  runWorkload,
	}
)

func init() {
	runCmd.Flags().IntVar(&numWorkers, "workers", 5, "Number of concurrent workers")
}

func runWorkload(cmd *cobra.Command, args []string) error {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	config := database.Config{
		DBHost:     dbHost,
		DBUser:     dbUser,
		DBPassword: dbPassword,
		DBName:     dbName,
		Seed:       seed,
	}

	db, err := database.Connect(config)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer db.Close()

	// Configure connection pool
	maxConns := numWorkers * 2
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 2)
	db.SetConnMaxLifetime(5 * time.Minute)

	log.Printf("Running continuous workload with %d workers... Press Ctrl+C to stop", numWorkers)
	return workload.Run(db, workload.Config{
		Workers:           numWorkers,
		Seed:              seed,
		Parents:           numParents,
		ChildrenPerParent: childrenPerParent,
	})
}
