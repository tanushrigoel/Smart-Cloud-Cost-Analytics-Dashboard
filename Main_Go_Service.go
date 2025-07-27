package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/gorilla/mux"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

// CostData represents billing data structure
type CostData struct {
	ProjectID    string    `json:"project_id" bigquery:"project_id"`
	ServiceName  string    `json:"service_name" bigquery:"service_description"`
	SKUName      string    `json:"sku_name" bigquery:"sku_description"`
	UsageDate    time.Time `json:"usage_date" bigquery:"usage_start_time"`
	Cost         float64   `json:"cost" bigquery:"cost"`
	Credits      float64   `json:"credits" bigquery:"credits"`
	Currency     string    `json:"currency" bigquery:"currency"`
	Location     string    `json:"location" bigquery:"location_location"`
}

// CostTrend represents cost trend analysis
type CostTrend struct {
	Date        time.Time `json:"date"`
	TotalCost   float64   `json:"total_cost"`
	ServiceCost map[string]float64 `json:"service_cost"`
	ProjectCost map[string]float64 `json:"project_cost"`
}

// Anomaly represents detected cost anomalies
type Anomaly struct {
	Date           time.Time `json:"date"`
	ProjectID      string    `json:"project_id"`
	ServiceName    string    `json:"service_name"`
	ActualCost     float64   `json:"actual_cost"`
	ExpectedCost   float64   `json:"expected_cost"`
	DeviationPct   float64   `json:"deviation_percentage"`
	Severity       string    `json:"severity"`
	Description    string    `json:"description"`
}

// CostAnalytics main service struct
type CostAnalytics struct {
	bqClient     *bigquery.Client
	sheetsClient *sheets.Service
	projectID    string
	datasetID    string
	tableID      string
}

// NewCostAnalytics creates a new analytics service
func NewCostAnalytics(projectID, datasetID, tableID string) (*CostAnalytics, error) {
	ctx := context.Background()
	
	// Initialize BigQuery client
	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	// Initialize Google Sheets client
	sheetsClient, err := sheets.NewService(ctx, option.WithScopes(sheets.SpreadsheetsScope))
	if err != nil {
		return nil, fmt.Errorf("failed to create Sheets client: %v", err)
	}

	return &CostAnalytics{
		bqClient:     bqClient,
		sheetsClient: sheetsClient,
		projectID:    projectID,
		datasetID:    datasetID,
		tableID:      tableID,
	}, nil
}

// GetCostTrends analyzes cost trends over time
func (ca *CostAnalytics) GetCostTrends(days int) ([]CostTrend, error) {
	ctx := context.Background()
	
	query := fmt.Sprintf(`
		WITH daily_costs AS (
			SELECT 
				DATE(usage_start_time) as usage_date,
				project.id as project_id,
				service.description as service_name,
				SUM(cost) as total_cost,
				SUM(IFNULL((SELECT SUM(amount) FROM UNNEST(credits)), 0)) as total_credits
			FROM %s.%s.%s
			WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL %d DAY)
			GROUP BY usage_date, project_id, service_name
		),
		aggregated_costs AS (
			SELECT 
				usage_date,
				SUM(total_cost + total_credits) as daily_total,
				ARRAY_AGG(STRUCT(service_name, total_cost + total_credits)) as service_costs,
				ARRAY_AGG(STRUCT(project_id, total_cost + total_credits)) as project_costs
			FROM daily_costs
			GROUP BY usage_date
			ORDER BY usage_date
		)
		SELECT * FROM aggregated_costs
	`, ca.projectID, ca.datasetID, ca.tableID, days)

	q := ca.bqClient.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}

	var trends []CostTrend
	for {
		var row struct {
			UsageDate    time.Time `bigquery:"usage_date"`
			DailyTotal   float64   `bigquery:"daily_total"`
			ServiceCosts []struct {
				ServiceName string  `bigquery:"service_name"`
				Cost        float64 `bigquery:"f1_"`
			} `bigquery:"service_costs"`
			ProjectCosts []struct {
				ProjectID string  `bigquery:"project_id"`
				Cost      float64 `bigquery:"f1_"`
			} `bigquery:"project_costs"`
		}

		err := it.Next(&row)
		if err != nil {
			break
		}

		serviceCosts := make(map[string]float64)
		for _, sc := range row.ServiceCosts {
			serviceCosts[sc.ServiceName] = sc.Cost
		}

		projectCosts := make(map[string]float64)
		for _, pc := range row.ProjectCosts {
			projectCosts[pc.ProjectID] = pc.Cost
		}

		trends = append(trends, CostTrend{
			Date:        row.UsageDate,
			TotalCost:   row.DailyTotal,
			ServiceCost: serviceCosts,
			ProjectCost: projectCosts,
		})
	}

	return trends, nil
}

// DetectAnomalies uses statistical analysis to detect cost anomalies
func (ca *CostAnalytics) DetectAnomalies(days int, threshold float64) ([]Anomaly, error) {
	ctx := context.Background()
	
	query := fmt.Sprintf(`
		WITH historical_data AS (
			SELECT 
				DATE(usage_start_time) as usage_date,
				project.id as project_id,
				service.description as service_name,
				SUM(cost + IFNULL((SELECT SUM(amount) FROM UNNEST(credits)), 0)) as daily_cost
			FROM %s.%s.%s
			WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL %d DAY)
			GROUP BY usage_date, project_id, service_name
		),
		stats AS (
			SELECT 
				project_id,
				service_name,
				AVG(daily_cost) as avg_cost,
				STDDEV(daily_cost) as stddev_cost,
				COUNT(*) as data_points
			FROM historical_data
			WHERE usage_date < DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
			GROUP BY project_id, service_name
			HAVING COUNT(*) >= 7
		),
		recent_costs AS (
			SELECT 
				DATE(usage_start_time) as usage_date,
				project.id as project_id,
				service.description as service_name,
				SUM(cost + IFNULL((SELECT SUM(amount) FROM UNNEST(credits)), 0)) as daily_cost
			FROM %s.%s.%s
			WHERE DATE(usage_start_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
			GROUP BY usage_date, project_id, service_name
		)
		SELECT 
			r.usage_date,
			r.project_id,
			r.service_name,
			r.daily_cost as actual_cost,
			s.avg_cost as expected_cost,
			s.stddev_cost,
			ABS(r.daily_cost - s.avg_cost) / NULLIF(s.stddev_cost, 0) as z_score
		FROM recent_costs r
		JOIN stats s ON r.project_id = s.project_id AND r.service_name = s.service_name
		WHERE ABS(r.daily_cost - s.avg_cost) / NULLIF(s.stddev_cost, 0) > %f
		ORDER BY z_score DESC
	`, ca.projectID, ca.datasetID, ca.tableID, days, ca.projectID, ca.datasetID, ca.tableID, threshold)

	q := ca.bqClient.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute anomaly query: %v", err)
	}

	var anomalies []Anomaly
	for {
		var row struct {
			UsageDate    time.Time `bigquery:"usage_date"`
			ProjectID    string    `bigquery:"project_id"`
			ServiceName  string    `bigquery:"service_name"`
			ActualCost   float64   `bigquery:"actual_cost"`
			ExpectedCost float64   `bigquery:"expected_cost"`
			StddevCost   float64   `bigquery:"stddev_cost"`
			ZScore       float64   `bigquery:"z_score"`
		}

		err := it.Next(&row)
		if err != nil {
			break
		}

		deviationPct := ((row.ActualCost - row.ExpectedCost) / row.ExpectedCost) * 100
		severity := "Medium"
		if row.ZScore > 3 {
			severity = "High"
		} else if row.ZScore > 2 {
			severity = "Medium"
		} else {
			severity = "Low"
		}

		description := fmt.Sprintf("Cost spike detected for %s in project %s. Expected: $%.2f, Actual: $%.2f",
			row.ServiceName, row.ProjectID, row.ExpectedCost, row.ActualCost)

		anomalies = append(anomalies, Anomaly{
			Date:           row.UsageDate,
			ProjectID:      row.ProjectID,
			ServiceName:    row.ServiceName,
			ActualCost:     row.ActualCost,
			ExpectedCost:   row.ExpectedCost,
			DeviationPct:   deviationPct,
			Severity:       severity,
			Description:    description,
		})
	}

	return anomalies, nil
}

// UpdateGoogleSheets updates the dashboard in Google Sheets
func (ca *CostAnalytics) UpdateGoogleSheets(spreadsheetID string, trends []CostTrend, anomalies []Anomaly) error {
	// Prepare trend data for sheets
	var trendValues [][]interface{}
	trendValues = append(trendValues, []interface{}{"Date", "Total Cost", "Top Service", "Top Service Cost"})
	
	for _, trend := range trends {
		topService := ""
		topCost := 0.0
		for service, cost := range trend.ServiceCost {
			if cost > topCost {
				topService = service
				topCost = cost
			}
		}
		
		trendValues = append(trendValues, []interface{}{
			trend.Date.Format("2006-01-02"),
			trend.TotalCost,
			topService,
			topCost,
		})
	}

	// Update trends sheet
	trendsRange := "Trends!A1"
	trendsVR := &sheets.ValueRange{
		Values: trendValues,
	}

	_, err := ca.sheetsClient.Spreadsheets.Values.Update(spreadsheetID, trendsRange, trendsVR).
		ValueInputOption("RAW").Do()
	if err != nil {
		return fmt.Errorf("failed to update trends sheet: %v", err)
	}

	// Prepare anomaly data for sheets
	var anomalyValues [][]interface{}
	anomalyValues = append(anomalyValues, []interface{}{
		"Date", "Project", "Service", "Actual Cost", "Expected Cost", "Deviation %", "Severity", "Description",
	})
	
	for _, anomaly := range anomalies {
		anomalyValues = append(anomalyValues, []interface{}{
			anomaly.Date.Format("2006-01-02"),
			anomaly.ProjectID,
			anomaly.ServiceName,
			anomaly.ActualCost,
			anomaly.ExpectedCost,
			fmt.Sprintf("%.1f%%", anomaly.DeviationPct),
			anomaly.Severity,
			anomaly.Description,
		})
	}

	// Update anomalies sheet
	anomaliesRange := "Anomalies!A1"
	anomaliesVR := &sheets.ValueRange{
		Values: anomalyValues,
	}

	_, err = ca.sheetsClient.Spreadsheets.Values.Update(spreadsheetID, anomaliesRange, anomaliesVR).
		ValueInputOption("RAW").Do()
	if err != nil {
		return fmt.Errorf("failed to update anomalies sheet: %v", err)
	}

	return nil
}

// ForecastCosts predicts future costs using simple linear regression
func (ca *CostAnalytics) ForecastCosts(days int) ([]CostTrend, error) {
	trends, err := ca.GetCostTrends(30) // Use 30 days of historical data
	if err != nil {
		return nil, err
	}

	if len(trends) < 7 {
		return nil, fmt.Errorf("insufficient data for forecasting")
	}

	// Simple linear regression for forecasting
	var sumX, sumY, sumXY, sumX2 float64
	n := float64(len(trends))

	for i, trend := range trends {
		x := float64(i)
		y := trend.TotalCost
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}

	// Calculate slope and intercept
	slope := (n*sumXY - sumX*sumY) / (n*sumX2 - sumX*sumX)
	intercept := (sumY - slope*sumX) / n

	// Generate forecasts
	var forecasts []CostTrend
	lastDate := trends[len(trends)-1].Date

	for i := 1; i <= days; i++ {
		x := float64(len(trends) + i - 1)
		predictedCost := slope*x + intercept
		
		// Ensure positive prediction
		if predictedCost < 0 {
			predictedCost = trends[len(trends)-1].TotalCost * 0.9
		}

		forecasts = append(forecasts, CostTrend{
			Date:      lastDate.AddDate(0, 0, i),
			TotalCost: predictedCost,
		})
	}

	return forecasts, nil
}

// HTTP Handlers
func (ca *CostAnalytics) handleGetTrends(w http.ResponseWriter, r *http.Request) {
	daysStr := r.URL.Query().Get("days")
	days := 30
	if daysStr != "" {
		if d, err := strconv.Atoi(daysStr); err == nil {
			days = d
		}
	}

	trends, err := ca.GetCostTrends(days)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(trends)
}

func (ca *CostAnalytics) handleGetAnomalies(w http.ResponseWriter, r *http.Request) {
	thresholdStr := r.URL.Query().Get("threshold")
	threshold := 2.0
	if thresholdStr != "" {
		if t, err := strconv.ParseFloat(thresholdStr, 64); err == nil {
			threshold = t
		}
	}

	anomalies, err := ca.DetectAnomalies(30, threshold)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(anomalies)
}

func (ca *CostAnalytics) handleGetForecast(w http.ResponseWriter, r *http.Request) {
	daysStr := r.URL.Query().Get("days")
	days := 7
	if daysStr != "" {
		if d, err := strconv.Atoi(daysStr); err == nil {
			days = d
		}
	}

	forecast, err := ca.ForecastCosts(days)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(forecast)
}

func (ca *CostAnalytics) handleUpdateSheets(w http.ResponseWriter, r *http.Request) {
	spreadsheetID := r.URL.Query().Get("spreadsheet_id")
	if spreadsheetID == "" {
		http.Error(w, "spreadsheet_id parameter required", http.StatusBadRequest)
		return
	}

	trends, err := ca.GetCostTrends(30)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	anomalies, err := ca.DetectAnomalies(30, 2.0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = ca.UpdateGoogleSheets(spreadsheetID, trends, anomalies)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
}

func main() {
	projectID := os.Getenv("GCP_PROJECT_ID")
	datasetID := os.Getenv("BQ_DATASET_ID")
	tableID := os.Getenv("BQ_TABLE_ID")
	port := os.Getenv("PORT")

	if projectID == "" || datasetID == "" || tableID == "" {
		log.Fatal("Environment variables GCP_PROJECT_ID, BQ_DATASET_ID, and BQ_TABLE_ID are required")
	}

	if port == "" {
		port = "8080"
	}

	analytics, err := NewCostAnalytics(projectID, datasetID, tableID)
	if err != nil {
		log.Fatalf("Failed to initialize analytics service: %v", err)
	}
	defer analytics.bqClient.Close()

	r := mux.NewRouter()
	r.HandleFunc("/api/trends", analytics.handleGetTrends).Methods("GET")
	r.HandleFunc("/api/anomalies", analytics.handleGetAnomalies).Methods("GET")
	r.HandleFunc("/api/forecast", analytics.handleGetForecast).Methods("GET")
	r.HandleFunc("/api/update-sheets", analytics.handleUpdateSheets).Methods("POST")

	// Health check endpoint
	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	}).Methods("GET")

	log.Printf("Starting Cost Analytics API server on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, r))
}