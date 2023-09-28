package internal

import (
	"encoding/csv"
	"io"
	"sync"

	mapset "github.com/deckarep/golang-set/v2"
)

type Processor struct {
	fs          FS
	transformer Transformer
	schema      Schema
	segmentSize int
	delimiter   rune
}

func NewProcessor(fs FS, transformer Transformer, schema Schema, segmentSize int, delimiter rune) Processor {
	printSegmentInfo(segmentSize)
	return Processor{fs, transformer, schema, segmentSize, delimiter}
}

func (p Processor) ProcessPartition(wg *sync.WaitGroup, batchNo int, partition string) {
	partitionFile := p.fs.openPartitionFile(partition)
	defer partitionFile.Close()

	processRecords := func(wg *sync.WaitGroup, records [][]string) {
		defer wg.Done()
		p.transformer.Transform(batchNo, records) // Layer 3
	}

	var records [][]string

	reader := csv.NewReader(partitionFile)
	reader.Comma = p.delimiter

	record, err := reader.Read()
	if err != nil {
		panic(err)
	}

	// Skip csv header (if present)
	recordSet := mapset.NewSet(record...)
	if !recordSet.Equal(p.schema.Header) {
		records = append(records, record)
	}

	for {
		record, err := reader.Read()

		if err == io.EOF {
			// Remaining records when window size is less than batch size
			if len(records) != 0 {
				wg.Add(1)
				go processRecords(wg, copySlice(records))
			}
			break
		} else if err != nil {
			panic(err)
		}

		records = append(records, record)

		if len(records) == p.segmentSize {
			wg.Add(1)
			go processRecords(wg, copySlice(records)) // Copy slice for goroutine
			records = records[:0]                     // Reset batch window
		}
	}
}

func CountBatches(n int, batchSize int) int {
	return n/batchSize + n%batchSize
}
