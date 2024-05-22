package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

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

func saveDataToPostgres(db *sql.DB, data []map[string]interface{}) error {
	for _, item := range data {
		// locationJSON, err := json.Marshal(item["location"])
		// if err != nil {
		// 	return fmt.Errorf("failed to marshal location to JSON: %v", err)
		// }

		_, err := db.Exec(`INSERT INTO permits (id, permit, permit_type, review_type, application_start_date, issue_date, processing_time, street_number, street_direction, street_name, work_description, building_fee_paid, zoning_fee_paid, other_fee_paid, subtotal_paid, building_fee_unpaid, zoning_fee_unpaid, other_fee_unpaid, subtotal_unpaid, building_fee_waived, building_fee_subtotal, zoning_fee_subtotal, other_fee_subtotal, zoning_fee_waived, other_fee_waived, subtotal_waived, total_fee, reported_cost,
			 contact_1_city, contact_1_name, contact_1_state, contact_1_type, contact_1_zipcode, latitude, longitude
			 , xcoordinate, ycoordinate) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24,
			 $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37)`,
			item["id"], item["permit"], item["permit_type"], item["review_type"],
			item["application_start_date"], item["issue_date"], item["processing_time"],
			item["street_number"], item["street_direction"], item["street_name"],
			item["work_description"], item["building_fee_paid"], item["zoning_fee_paid"],
			item["other_fee_paid"], item["subtotal_paid"], item["building_fee_unpaid"],
			item["zoning_fee_unpaid"], item["other_fee_unpaid"], item["subtotal_unpaid"],
			item["building_fee_waived"], item["building_fee_subtotal"], item["zoning_fee_subtotal"],
			item["other_fee_subtotal"], item["zoning_fee_waived"], item["other_fee_waived"],
			item["subtotal_waived"], item["total_fee"], item["reported_cost"],
			item["contact_1_city"], item["contact_1_name"], item["contact_1_state"],
			item["contact_1_type"], item["contact_1_zipcode"], item["latitude"],
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

	if err := saveDataToPostgres(db, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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

	http.HandleFunc("/insert-data", func(w http.ResponseWriter, r *http.Request) {
		handler(db, w, r)
	})
	port := "8081"
	fmt.Printf("Insert service is listening on port %s...\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
