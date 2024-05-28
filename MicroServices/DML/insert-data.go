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

		_, err := db.Exec(`INSERT INTO Building_Permits_Fact  (id, permit, permit_type, application_start_date, latitude, longitude
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

func saveTaxiTripsToPostgres(db *sql.DB, data []map[string]interface{}) error {
	for _, item := range data {
		_, err := db.Exec(`INSERT INTO Taxi_Trips_Fact (trip_id, trip_start_timestamp, trip_end_timestamp, 
                            pickup_community_area, dropoff_community_area, pickup_centroid_latitude,
                            pickup_centroid_longitude, dropoff_centroid_latitude, dropoff_centroid_longitude) 
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            ON CONFLICT (trip_id) DO NOTHING`,
			item["trip_id"], item["trip_start_timestamp"], item["trip_end_timestamp"],
			item["pickup_community_area"], item["dropoff_community_area"],
			item["pickup_centroid_latitude"], item["pickup_centroid_longitude"],
			item["dropoff_centroid_latitude"], item["dropoff_centroid_longitude"])

		if err != nil {
			return fmt.Errorf("failed to insert data: %v", err)
		}
	}
	return nil
}

func saveHealthStatisticsToPostgres(db *sql.DB, data []map[string]interface{}) error {
	for _, item := range data {

		_, err := db.Exec(`INSERT INTO public_health_statistics (zip_code, cases_cumulative, cases_weekly, week_number, week_start, week_end, case_rate_weekly) 
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			item["zip_code"], item["cases_cumulative"], item["cases_weekly"],
			item["week_number"], item["week_start"], item["week_end"], item["case_rate_weekly"])

		if err != nil {
			return fmt.Errorf("failed to insert data: %v", err)
		}
	}
	return nil
}

func saveCovid19ToPostgres(db *sql.DB, data []map[string]interface{}) error {
	for _, item := range data {
		_, err := db.Exec(`INSERT INTO COVID_Cases_Fact (geography_type, community_area_or_zip, ccvi_score, ccvi_category) 
                           VALUES ($1, $2, $3, $4)
                           `,
			item["geography_type"], item["community_area_or_zip"], item["ccvi_score"],
			item["ccvi_category"])

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
	if strings.Contains(r.URL.String(), "insert-taxi-trips") {
		if err := saveTaxiTripsToPostgres(db, data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if strings.Contains(r.URL.String(), "insert-health-statistics") {
		if err := saveHealthStatisticsToPostgres(db, data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if strings.Contains(r.URL.String(), "insert-covid-cases") {
		if err := saveCovid19ToPostgres(db, data); err != nil {
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

	http.HandleFunc("/insert-taxi-trips", func(w http.ResponseWriter, r *http.Request) {
		handler(db, w, r)
	})

	http.HandleFunc("/insert-health-statistics", func(w http.ResponseWriter, r *http.Request) {
		handler(db, w, r)
	})

	http.HandleFunc("/insert-covid-cases", func(w http.ResponseWriter, r *http.Request) {
		handler(db, w, r)
	})

	port := "8081"
	fmt.Printf("Insert service is listening on port %s...\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
