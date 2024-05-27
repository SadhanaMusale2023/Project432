package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	_ "github.com/lib/pq"
)

// PostgreSQL connection details
const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "postgres"
	dbname   = "chicago"
)

func saveBuildingDataToPostgres(db *sql.DB, data []map[string]interface{}) error {
	for _, item := range data {

		// TODO handle this
		// locationJSON, err := json.Marshal(item["location"])
		// if err != nil {
		// 	return fmt.Errorf("failed to marshal location to JSON: %v", err)
		// }

		_, err := db.Exec(`INSERT INTO permits (id, permit, permit_type, application_start_date, latitude, longitude
			 , xcoordinate, ycoordinate) 
		    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			item["id"], item["permit"], item["permit_type"],
			item["application_start_date"],
			item["latitude"],
			item["longitude"], item["xcoordinate"], item["ycoordinate"])

		if err != nil {
			return fmt.Errorf("failed to insert data: %v", err)
		}
	}
	return nil
}

// HTTP handler function
func handler(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	var data []map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "failed to decode request body", http.StatusBadRequest)
		return
	}

	if strings.Contains(r.URL.String(), "insert-building-permit-data") {
		if err := saveBuildingDataToPostgres(db, data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Data inserted successfully"))
}

func main() {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	http.HandleFunc("/insert-building-permit-data", func(w http.ResponseWriter, r *http.Request) {
		handler(db, w, r)
	})
	port := "8081"
	fmt.Printf("Insert service is listening on port %s...\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
