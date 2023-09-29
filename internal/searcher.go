package internal

import (
	"os"
	"strings"
)

type Searcher interface {
	Cleaner
	SegmentsProcessor
}

type ColumnSearcher struct {
	pattern    string
	outputFile *os.File
	matches    chan []string
}

func NewSearcher(schema Schema, pattern string, outputPath string) Searcher {
	if pattern == "" {
		panic("pattern string should not be empty")
	}

	// Delete existing output file
	if pathExists(outputPath) {
		os.RemoveAll(outputPath)
	}

	matches := make(chan []string)

	file := appendToFile(outputPath, matches)
	matches <- schema.Columns // Append header

	printSearchInfo(pattern, outputPath)
	return ColumnSearcher{pattern, file, matches}
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
