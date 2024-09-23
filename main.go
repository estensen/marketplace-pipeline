package main

import (
	"fmt"
	"os"

	"os/exec"
)

const csvFile = "sample_data.csv"

func main() {
	cmd := exec.Command("./clickhouse", "local",
		"--structure", "date Date, project_id String, transactions_count UInt64, total_volume_usd Float64",
		"--input-format", "CSV",
		"--file", csvFile,
		"--query", "SELECT * FROM file('"+csvFile+"')")

	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("Error running clickhouse-local: %v\n", err)
		fmt.Printf("Output: %s\n", string(output))
		os.Exit(1)
	}

	fmt.Printf("Clickhouse-local output: %s\n", string(output))
}
