// cmd/workload/main.go

package main

import (
	"log"
	"os"

	"github.com/shailendra-patel/mysql-workload/cmd/workload/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		log.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
