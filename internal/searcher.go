package internal

import (
	"encoding/csv"
	"os"
	"strings"
)

type Searcher interface {
	Cleaner
	SegmentsProcessor
}

type ColumnSearcher struct {
	fs         FS
	pattern    string
	outputFile *os.File
	matches    chan []string
}

func NewSearcher(fs FS, schema Schema, pattern string, outputPath string) Searcher {
	if pattern == "" {
		panic("pattern string should not be empty")
	}

	// Delete existing output file
	if pathExists(outputPath) {
		os.RemoveAll(outputPath)
	}

	file, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}

	matches := make(chan []string)

	go appendToFile(file, matches)
	matches <- schema.Columns // Append header

	printSearchInfo(pattern, outputPath)
	return ColumnSearcher{fs, pattern, file, matches}
}

func appendToFile(file *os.File, data chan []string) { // Move to fs
	writer := csv.NewWriter(file)
	defer writer.Flush() // TODO: flush in batches, why is header skipped?

	for row := range data {
		if err := writer.Write(row); err != nil {
			panic(err)
		}

	}
}

func (cs ColumnSearcher) ProcessSegment(batchNo int, records [][]string) {
	for _, row := range records {
		for _, col := range row {
			if strings.Contains(col, cs.pattern) {
				cs.matches <- row
			}
		}
	}
}

func (cs ColumnSearcher) Cleanup() {
	close(cs.matches)
	cs.outputFile.Close()
}
