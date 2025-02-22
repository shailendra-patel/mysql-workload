package cmd

import (
	"fmt"
	"log"
	"time"

	"github.com/shailendra-patel/mysql-workload/internal/database"
	"github.com/spf13/cobra"
)

var (
	initCmd = &cobra.Command{
		Use:   "init",
		Short: "Initialize the database with test data",
		Long:  `Creates necessary tables and populates them with initial test data.`,
		RunE:  runInit,
	}
)

func runInit(cmd *cobra.Command, args []string) error {
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

	log.Println("Creating tables...")
	if err := database.CreateTables(db); err != nil {
		return fmt.Errorf("creating tables: %w", err)
	}

	log.Println("Initializing data...")
	if err := database.InitDatabase(db, config, numParents, childrenPerParent); err != nil {
		return fmt.Errorf("initializing database: %w", err)
	}

	log.Println("Initialization complete")
	return nil
}
