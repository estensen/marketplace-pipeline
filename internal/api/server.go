package api

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/estensen/marketplace-pipeline/internal/aggregator"
)

// Server represents the API server with necessary dependencies.
type Server struct {
	Aggregator *aggregator.Aggregator
	Conn       clickhouse.Conn
}

// NewServer initializes a new API server instance.
func NewServer(agg *aggregator.Aggregator, conn clickhouse.Conn) *Server {
	return &Server{
		Aggregator: agg,
		Conn:       conn,
	}
}

// CalculateMetricsHandler handles the /metrics endpoint.
func (s *Server) CalculateMetricsHandler(w http.ResponseWriter, r *http.Request) {
	// Parse date from query parameters
	dateStr := r.URL.Query().Get("date")
	if dateStr == "" {
		http.Error(w, "Missing 'date' query parameter", http.StatusBadRequest)
		return
	}

	date, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		http.Error(w, "Invalid date format. Use YYYY-MM-DD.", http.StatusBadRequest)
		return
	}

	// Calculate metrics
	metrics, err := s.Aggregator.CalculateMetrics(s.Conn, date)
	if err != nil {
		log.Printf("Error calculating metrics: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Respond with JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		log.Printf("Error encoding response: %v", err)
	}
}

// StartServer initializes and starts the API server.
func StartServer(addr string, server *Server) {
	http.HandleFunc("/metrics", server.CalculateMetricsHandler)

	log.Printf("API server is running on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Failed to start API server: %v", err)
	}
}
