package api

import (
	"log"
	"net/http"
)

// StartServer initializes and starts the API server.
func StartServer(addr string, server *Server) {
	http.HandleFunc("/metrics", server.CalculateMetricsHandler)

	log.Printf("API server is running on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Failed to start API server: %v", err)
	}
}
