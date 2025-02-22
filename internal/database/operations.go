// internal/database/operations.go

package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

type Config struct {
	DBHost     string
	DBUser     string
	DBPassword string
	DBName     string
	Seed       int64
}

type ParentRecord struct {
	ID    int64
	Name  string
	Value float64
}

type ChildRecord struct {
	ID       int64
	ParentID int64
	Name     string
	Value    float64
}

// Connect establishes a connection to the database using the provided configuration
func Connect(config Config) (*sql.DB, error) {

	// Enable query logging in MySQL driver
	cfg := mysql.Config{
		User:                 config.DBUser,
		Passwd:               config.DBPassword,
		Net:                  "tcp",
		Addr:                 config.DBHost,
		DBName:               config.DBName,
		AllowNativePasswords: true,
		InterpolateParams:    true,
		// Enable query logging
		Logger: log.New(log.Writer(), "[MySQL] ", log.LstdFlags),
	}
	//dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", config.DBUser, config.DBPassword, config.DBHost, config.DBName)
	return sql.Open("mysql", cfg.FormatDSN())
}

// CreateTables creates the necessary database tables if they don't exist
func CreateTables(db *sql.DB) error {
	// Drop existing tables
	_, err := db.Exec(`DROP TABLE IF EXISTS child_records`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`DROP TABLE IF EXISTS parent_records`)
	if err != nil {
		return err
	}

	// Create parent_records table
	_, err = db.Exec(`
        CREATE TABLE parent_records (
            id BIGINT NOT NULL,
            var_char_id VARCHAR(36) NOT NULL,
            varchar_field VARCHAR(1000) NOT NULL,
            text_field TEXT NOT NULL,
            tinyint_field TINYINT(1) NOT NULL DEFAULT 0,
            timestamp_field TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            json_field JSON NOT NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    `)
	if err != nil {
		return err
	}

	// Create child_records table
	_, err = db.Exec(`
        CREATE TABLE child_records (
            id BIGINT NOT NULL,
            parent_id BIGINT NOT NULL,
            varchar_field VARCHAR(1000) NOT NULL,
            bigint_field BIGINT DEFAULT NULL,
            active_field TINYINT(1) NOT NULL DEFAULT 1,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP NULL DEFAULT NULL,
            metadata TEXT NOT NULL,
            PRIMARY KEY (id),
            KEY parent_id_idx (parent_id),
            CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) 
                REFERENCES parent_records (id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    `)
	return err
}

// InsertNewParentWithChildren inserts a new parent record with random children
func InsertNewParentWithChildren(db *sql.DB, r *rand.Rand, childrenPerParent int) error {
	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Prepare statements
	parentStmt, err := tx.Prepare(`
        INSERT INTO parent_records (
            id, var_char_id, varchar_field, text_field, tinyint_field,
            timestamp_field, json_field, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `)
	if err != nil {
		return err
	}
	defer parentStmt.Close()

	childStmt, err := tx.Prepare(`
        INSERT INTO child_records (
            id, parent_id, varchar_field, bigint_field, active_field,
            created_at, expires_at, metadata
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `)
	if err != nil {
		return err
	}
	defer childStmt.Close()

	// Insert parent records

	//parentID := generateRandomString(r, 36)
	parentID := r.Int()
	now := time.Now()
	jsonData := generateLargeJSON(r)
	jsonBytes, err := json.Marshal(jsonData)
	if err != nil {
		return err
	}

	_, err = parentStmt.Exec(
		parentID,
		generateRandomString(r, 36),
		generateRandomString(r, 1000),
		generateRandomString(r, 5000),
		r.Intn(2) == 1,
		now,
		string(jsonBytes),
		now,
		now,
	)
	if err != nil {
		return err
	}

	// Insert child records
	for j := 0; j < childrenPerParent; j++ {
		childID := r.Int()
		createdAt := now.Add(time.Duration(r.Intn(86400)) * time.Second)
		expiresAt := createdAt.Add(time.Duration(r.Intn(365)) * 24 * time.Hour)

		_, err = childStmt.Exec(
			childID,
			parentID,
			generateRandomString(r, 1000),
			r.Int63n(1000000000),
			r.Intn(2) == 1,
			createdAt,
			expiresAt,
			generateRandomString(r, 5000),
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// UpdateRandomParent updates a random parent record with a new value
func UpdateRandomParent(db *sql.DB, r *rand.Rand) error {
	result, err := db.Exec(
		"UPDATE parent_records SET int_field = ? WHERE id = (SELECT id FROM (SELECT id FROM parent_records ORDER BY RAND() LIMIT 1) tmp)",
		r.Float64()*1000,
	)
	if err != nil {
		return fmt.Errorf("updating random parent: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("getting rows affected: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("no rows updated")
	}

	return nil
}

// DeleteRandomParent deletes a random parent record and its children
func DeleteRandomParent(db *sql.DB, r *rand.Rand) error {
	result, err := db.Exec(
		"DELETE FROM parent_records WHERE id = (SELECT id FROM (SELECT id FROM parent_records ORDER BY RAND() LIMIT 1) tmp)",
	)
	if err != nil {
		return fmt.Errorf("deleting random parent: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("getting rows affected: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("no rows deleted")
	}

	return nil
}

// InitDatabase initializes the database with the specified number of records
func InitDatabase(db *sql.DB, config Config, numParents, childrenPerParent int) error {
	r := rand.New(rand.NewSource(config.Seed))

	for i := 0; i < numParents; i++ {
		if err := InsertNewParentWithChildren(db, r, childrenPerParent); err != nil {
			return fmt.Errorf("inserting parent %d: %w", i, err)
		}
		if i > 0 && i%10 == 0 {
			log.Printf("Inserted %d parents...", i)
		}
	}
	return nil
}

// Generate a random string of specified length
func generateRandomString(r *rand.Rand, length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[r.Intn(len(charset))]
	}
	return string(b)
}

// Generate an array of random strings
func generateRandomStringArray(r *rand.Rand, count, length int) []string {
	result := make([]string, count)
	for i := range result {
		result[i] = generateRandomString(r, length)
	}
	return result
}

// Generate a large JSON object with nested structures
func generateLargeJSON(r *rand.Rand) map[string]interface{} {
	// Create an array of user-like objects
	users := make([]map[string]interface{}, 2)
	for i := 0; i < 2; i++ {
		users[i] = map[string]interface{}{
			"id":     generateRandomString(r, 36),
			"name":   generateRandomString(r, 50),
			"email":  fmt.Sprintf("%s@%s.com", generateRandomString(r, 10), generateRandomString(r, 5)),
			"age":    r.Intn(100),
			"active": r.Intn(2) == 1,
			"metadata": map[string]interface{}{
				"lastLogin":  time.Now().Add(-time.Duration(r.Intn(10000)) * time.Hour).Format(time.RFC3339),
				"loginCount": r.Intn(1000),
				"preferences": map[string]interface{}{
					"theme":      generateRandomString(r, 10),
					"language":   generateRandomString(r, 5),
					"timezone":   generateRandomString(r, 30),
					"newsletter": r.Intn(2) == 1,
				},
			},
			"tags": generateRandomStringArray(r, 2, 10),
		}
	}

	return map[string]interface{}{
		"metadata": map[string]interface{}{
			"version":    "1.0",
			"generated":  time.Now().Format(time.RFC3339),
			"recordType": "user_batch",
		},
		"users": users,
		"settings": map[string]interface{}{
			"batchSize": 100,
			"timestamp": time.Now().Unix(),
			"flags":     generateRandomStringArray(r, 3, 20),
		},
	}
}
