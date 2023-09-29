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

func NewSearcher(fs FS, schema Schema, pattern string, outputPath string) ColumnSearcher {
	if pattern == "" {
		panic("pattern string should not be empty")
	}

	// Delete existing output file
	if pathExists(outputPath) {
		os.RemoveAll(outputPath)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	file := fs.getSearchResultsFile(outputPath)
	writer := csv.NewWriter(file)

	printSearchInfo(pattern, outputPath)

	cs := ColumnSearcher{&wg, pattern, file, writer, &mu}
	cs.appendRow(schema.Columns) // Append header

	return cs
}

func (cs *ColumnSearcher) appendRow(row []string) {
	cs.mu.Lock()
	if err := cs.writer.Write(row); err != nil {
		panic(err)
	}
	cs.mu.Unlock()
}

func (cs ColumnSearcher) ProcessSegment(batchNo int, records [][]string) {
	for _, row := range records {
		for _, col := range row {
			if strings.Contains(col, cs.pattern) {
				cs.appendRow(row)
			}
		}
	}
}

func (cs ColumnSearcher) Cleanup() {
	cs.writer.Flush()
	cs.outputFile.Close()
}
