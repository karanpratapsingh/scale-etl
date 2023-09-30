package internal

import (
	"encoding/csv"
	"os"
	"strings"
	"sync"
)

type Searcher interface {
	Cleaner
	SegmentProcessor
}

type ColumnSearcher struct {
	wg         *sync.WaitGroup
	pattern    string
	outputFile *os.File
	writer     *csv.Writer
	mu         *sync.Mutex
}

func NewSearcher(schema Schema, pattern string, outputPath string) ColumnSearcher {
	if pattern == "" { // TODO: move to errors
		panic("pattern string should not be empty")
	}

	// Delete existing output file
	if pathExists(outputPath) {
		os.RemoveAll(outputPath)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	file := getSearchResultsFile(outputPath)
	writer := csv.NewWriter(file)

	printSearchInfo(pattern, outputPath)

	cs := ColumnSearcher{&wg, pattern, file, writer, &mu}
	cs.appendRow(schema.Columns) // Append header

	return cs
}

func (cs ColumnSearcher) BatchComplete(int) {
	cs.mu.Lock()
	cs.writer.Flush()
	cs.mu.Unlock()
}

func (cs ColumnSearcher) ProcessSegment(batchNo int, rows []Row) {
	for _, row := range rows {
		for _, col := range row {
			if strings.Contains(col, cs.pattern) { // TODO: implement pattern matching
				cs.appendRow(row)
			}
		}
	}

}

func (cs ColumnSearcher) Cleanup() error {
	cs.mu.Lock()
	cs.writer.Flush()
	cs.mu.Unlock()

	return cs.outputFile.Close()
}

func (cs *ColumnSearcher) appendRow(row []string) {
	cs.mu.Lock()
	if err := cs.writer.Write(row); err != nil {
		panic(err)
	}
	cs.mu.Unlock()
}
