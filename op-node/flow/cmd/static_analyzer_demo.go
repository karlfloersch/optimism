package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ethereum-optimism/optimism/op-node/flow"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run static_analyzer_demo.go <directory>")
		fmt.Println("Example: go run static_analyzer_demo.go ../rollup")
		os.Exit(1)
	}

	directory := os.Args[1]

	// Make path absolute for better output
	absDir, err := filepath.Abs(directory)
	if err != nil {
		fmt.Printf("Error getting absolute path: %v\n", err)
		absDir = directory
	}

	fmt.Printf("🚀 Starting Static Analysis of Go Event System\n")
	fmt.Printf("📁 Target Directory: %s\n\n", absDir)

	// Create and run the analyzer
	analyzer := flow.NewStaticAnalyzer()

	err = analyzer.AnalyzeDirectory(directory)
	if err != nil {
		fmt.Printf("❌ Analysis failed: %v\n", err)
		os.Exit(1)
	}

	// Show results
	analyzer.PrintResults()

	// Build event mappings
	mappings := analyzer.BuildEventMappings()

	fmt.Printf("\n🗺️  Generated Event Mappings:\n")
	for eventType, mapping := range mappings {
		fmt.Printf("  📋 %s:\n", eventType)
		fmt.Printf("    Handler: %s\n", mapping.HandlerFunc)
		fmt.Printf("    Source: %s\n", mapping.SourceFile)
		fmt.Printf("    Consumes: %v\n", mapping.Consumed)
		fmt.Printf("    Produces: %v\n", mapping.Produced)
		fmt.Printf("\n")
	}

	fmt.Printf("✅ Analysis complete! Found %d event types\n", len(mappings))
}
