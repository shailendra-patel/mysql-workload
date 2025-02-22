// internal/workload/workload.go

package workload

import (
	"context"
	"database/sql"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/shailendra-patel/mysql-workload/internal/database"
)

type Config struct {
	Workers           int
	Seed              int64
	Parents           int
	ChildrenPerParent int
}

type Stats struct {
	InsertCount uint64
	UpdateCount uint64
	DeleteCount uint64
	ErrorCount  uint64
	incrChan    chan string
}

func (s *Stats) Increment(ctx context.Context, child uint64) {
	for {
		select {
		case <-ctx.Done():
			log.Println("Stats updater shutting down.")
			return
		case op := <-s.incrChan:
			switch op {
			case "insert":
				s.InsertCount += child + 1
			case "update":
				s.UpdateCount++
			case "delete":
				s.DeleteCount++
			case "error":
				s.ErrorCount++
			}
		}
	}
}

func (s *Stats) Print() {
	log.Printf("Workload Statistics:\n"+
		"- Inserts: %d\n"+
		"- Updates: %d\n"+
		"- Deletes: %d\n"+
		"- Errors:  %d\n",
		s.InsertCount, s.UpdateCount, s.DeleteCount, s.ErrorCount)
}

func worker(ctx context.Context, db *sql.DB, stats *Stats, seed int64, workerID, childrenPerParent int) {
	r := rand.New(rand.NewSource(seed + int64(workerID)))

	for {
		select {
		case <-ctx.Done():
			return
		default:
			v := r.Intn(100)
			switch {
			case v < 100:
				if err := database.InsertNewParentWithChildren(db, r, childrenPerParent); err != nil {
					log.Printf("Worker %d - Error inserting: %v", workerID, err)
					stats.incrChan <- "error"
				} else {
					stats.incrChan <- "insert"
				}
				//case v >= 70 && v < 90:
				//	if err := database.UpdateRandomParent(db, r); err != nil {
				//		log.Printf("Worker %d - Error updating: %v", workerID, err)
				//		stats.Increment("error")
				//	} else {
				//		stats.Increment("update")
				//	}
				//default:
				//	if err := database.DeleteRandomParent(db, r); err != nil {
				//		log.Printf("Worker %d - Error deleting: %v", workerID, err)
				//		stats.Increment("error")
				//	} else {
				//		stats.Increment("delete")
				//	}
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond * time.Duration(r.Intn(1000))):
			}
		}
	}
}

func Run(db *sql.DB, config Config) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	stats := &Stats{incrChan: make(chan string, config.Workers)}
	var wg sync.WaitGroup

	// Start stats printer
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				stats.Print()
				return
			case <-ticker.C:
				stats.Print()
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		stats.Increment(ctx, uint64(config.ChildrenPerParent))
	}()

	// Start workers
	for i := 0; i < config.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			worker(ctx, db, stats, config.Seed, workerID, config.ChildrenPerParent)
		}(i)
	}

	// Wait for shutdown signal
	sig := <-sigChan
	log.Printf("Received signal %v, initiating graceful shutdown...", sig)

	cancel()

	shutdownChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(shutdownChan)
	}()

	select {
	case <-shutdownChan:
		log.Println("Graceful shutdown completed")
	case <-time.After(30 * time.Second):
		log.Println("Shutdown timed out after 30 seconds")
	}

	return nil
}
