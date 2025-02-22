# MySQL Workload Generator

MySQL Workload Generator is a command-line tool designed to simulate MySQL operations for testing and development purposes. It allows you to easily initialize your database schema and data and then generate a mix of SQL queries to mimic typical workload scenarios against your MySQL database.

## Features

- **Database Initialization:**  
  Use the `init` command to create tables and seed initial data in your MySQL database.
- **Workload Simulation:**  
  The `run` command generates a variety of SQL operations (insert, update, and delete queries) to simulate a realistic workload on your database.

## Installation

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/shailendra-patel/mysql-workload.git
   cd mysql-workload
   ```
2. **Download Dependencies:**
   ```bash
   go mod download
   ```
3. **Build the Application:**
   ```bash
   go build -o mysql-workload ./cmd/workload/main.go
   # Building for linux on mac
   GOOS=linux GOARCH=amd64 go build -o mysql-workload ./cmd/workload/main.go
   ```
4. **Usage**
   ```bash
   # create tables and initialize data
   mysql-workload init --dbname=test_db --host=localhost --user=root --password=xyz
   # run the workload generator
   mysql-workload run --workers=5 -dbname=test_db --host=localhost --user=root --password=xyz
   
   ```