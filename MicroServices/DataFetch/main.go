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

// Fetch data from an external HTTP endpoint
// func fetchDataFromEndpoint(url string) ([]map[string]interface{}, error) {
// 	var allResults []map[string]interface{}
// 	page := 0
// 	limit := 50000
// 	for {
// 		// Construct the paginated URL
// 		//https://data.cityofchicago.org/resource/ydr8-5enu.json?$offset=100&$limit=50000
// 		paginatedURL := fmt.Sprintf("%s?$offset=%d&$limit=%d", url, page*limit, limit)
// 		resp, err := http.Get(paginatedURL)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to fetch data: %v", err)
// 		}
// 		defer resp.Body.Close()

// 		// Decode the response body into a slice of maps
// 		var results []map[string]interface{}
// 		if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
// 			return nil, fmt.Errorf("failed to decode response: %v", err)
// 		}

// 		// If no more results, break the loop
// 		if len(results) < limit {
// 			break
// 		}

// 		// Append results to the allResults slice
// 		allResults = append(allResults, results...)
// 		page++
// 	}

// 	return allResults, nil
// }

func fetchDataFromEndpoint(url string) ([]map[string]interface{}, error) {
	start := time.Now()
	insertServiceURL := "http://localhost:8081/insert-data"
	limit := 50000
	var allResults []map[string]interface{}
	page := 0
	resultsCh := make(chan []map[string]interface{}, 10) // Buffered channel to store results
	errorCh := make(chan error)
	var wg sync.WaitGroup
	maxGoroutines := 15
	columns := "id,permit_type,application_start_date,issue_date,processing_time,latitude,longitude,xcoordinate,ycoordinate"
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

				if len(results) < limit {
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
func handler(w http.ResponseWriter, r *http.Request) {
	externalURL := "https://data.cityofchicago.org/resource/ydr8-5enu.json" // Replace with the actual URL
	//start := time.Now()
	//fmt.Printf("fetch %d \n", page)
	data, err := fetchDataFromEndpoint(externalURL)
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
	http.HandleFunc("/fetch-data", handler)

	runtime.GOMAXPROCS(1) // Optional: Limit Go to use 1 core
	port := "8080"
	fmt.Printf("Server is listening on port %s...\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
