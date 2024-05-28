package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"
)

func fetchDataFromEndpoint(url string, columns string, maxGoroutines int, insertServiceURL string) ([]map[string]interface{}, error) {
	start := time.Now()

	limit := 50000
	var allResults []map[string]interface{}
	page := 0
	resultsCh := make(chan []map[string]interface{}, 10) // Buffered channel to store results
	errorCh := make(chan error)
	var wg sync.WaitGroup

	semaphore := make(chan struct{}, maxGoroutines)

	// Function to fetch a single page of data
	fetchPage := func(page int) {
		defer wg.Done()
		offset := page * limit
		paginatedURL := fmt.Sprintf("%s?$offset=%d&$limit=%d&$select=%s", url, offset, limit, columns)
		resp, err := http.Get(paginatedURL)
		if err != nil {
			errorCh <- fmt.Errorf("failed to fetch data: %v", err)
			return
		}
		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			errorCh <- fmt.Errorf("failed to fetch data: status code %d, body: %s", resp.StatusCode, string(bodyBytes))
			return
		}

		defer resp.Body.Close()

		var results []map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
			errorCh <- fmt.Errorf("failed to decode response: %v", err)
			return
		}

		fmt.Printf("fetch %d \n", page)

		resultsCh <- results
		<-semaphore
	}

	// Start initial fetches
	for i := 0; i < maxGoroutines; i++ {
		wg.Add(1)
		semaphore <- struct{}{}
		go fetchPage(page)
		page++
	}

	// Close channels when done
	go func() {
		wg.Wait()
		close(resultsCh)
		close(errorCh)
	}()

	// Collect results and errors
	for {
		select {
		case results, ok := <-resultsCh:
			if ok {

				fmt.Printf("forwardDataToInsertService %d \n", page)
				if err := forwardDataToInsertService(insertServiceURL, results); err != nil {
					// http.Error(w, err.Error(), http.StatusInternalServerError)
					return nil, err
				}

				if len(results) < limit && len(results) > 0 {
					// If the results are less than the limit, we can stop early
					return nil, nil
				}

				// Fetch next page
				wg.Add(1)

				semaphore <- struct{}{}
				go fetchPage(page)
				page++
			} else {
				resultsCh = nil
			}
		case err, ok := <-errorCh:
			if ok {
				return nil, err
			} else {
				errorCh = nil
			}
		}

		// Break if both channels are closed
		if resultsCh == nil && errorCh == nil {
			break
		}
	}

	end := time.Now()
	duration := end.Sub(start)

	fmt.Printf("Data fetched and decoded successfully. Duration: %v\n", duration)

	fmt.Print("return from fetch")
	return allResults, nil
}

func forwardDataToInsertService(url string, data []map[string]interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to forward data: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code: %d, response: %s", resp.StatusCode, string(body))
	}

	return nil
}

// HTTP handler function
func buildingPermithandler(w http.ResponseWriter, r *http.Request) {
	externalURL := "https://data.cityofchicago.org/resource/ydr8-5enu.json" // Replace with the actual URL
	columns := "id,permit_type,application_start_date,issue_date,processing_time,latitude,longitude,xcoordinate,ycoordinate"
	insertServiceURL := "http://localhost:8081/insert-building-permit-data"
	fetch(w, r, externalURL, columns, 15, insertServiceURL)

}

func taxiTripsHandler(w http.ResponseWriter, r *http.Request) {
	externalURL := "https://data.cityofchicago.org/resource/m6dm-c72p.json" // Replace with the actual URL
	columns := "trip_id,trip_start_timestamp,trip_end_timestamp,pickup_community_area,dropoff_community_area,pickup_centroid_latitude,pickup_centroid_longitude,dropoff_centroid_latitude,dropoff_centroid_longitude"
	insertServiceURL := "http://localhost:8081/insert-taxi-trips"
	fetch(w, r, externalURL, columns, 25, insertServiceURL)

}

func healthStatisticsHandler(w http.ResponseWriter, r *http.Request) {
	externalURL := "https://data.cityofchicago.org/resource/yhhz-zm2v.json" // Replace with the actual URL
	columns := "zip_code,cases_cumulative,cases_weekly,week_number,week_start,week_end,case_rate_weekly"
	insertServiceURL := "http://localhost:8081/insert-health-statistics"
	fetch(w, r, externalURL, columns, 1, insertServiceURL)
}

func covid19ccvHandler(w http.ResponseWriter, r *http.Request) {
	externalURL := "https://data.cityofchicago.org/resource/2ns9-phjk.json" // Replace with the actual URL
	columns := "geography_type,community_area_or_zip,ccvi_score,ccvi_category"
	insertServiceURL := "http://localhost:8081/insert-covid-cases"
	fetch(w, r, externalURL, columns, 1, insertServiceURL)
}

func fetch(w http.ResponseWriter, r *http.Request, externalURL string, columns string, maxRountine int, insertServiceURL string) {
	data, err := fetchDataFromEndpoint(externalURL, columns, maxRountine, insertServiceURL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response, err := json.Marshal(data)
	if err != nil {
		http.Error(w, "failed to marshal response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(response)

}

func main() {
	http.HandleFunc("/fetch-building-permits", buildingPermithandler)
	http.HandleFunc("/fetch-taxi-trips", taxiTripsHandler)
	http.HandleFunc("/fetch-health-statistics", healthStatisticsHandler)
	http.HandleFunc("/fetch-covid-cases", covid19ccvHandler)
	runtime.GOMAXPROCS(2) // Optional: Limit Go to use 1 core
	port := "8080"
	fmt.Printf("Server is listening on port %s...\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
