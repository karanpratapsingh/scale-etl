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
}

func NewProcessor(fs FS, transformer Transformer) Processor {
	return Processor{fs, transformer}
}

func (p Processor) ProcessPartition(wg *sync.WaitGroup, partition string, schema Schema, segmentSize int, delimiter rune) {
	partitionFile := p.fs.OpenPartitionFile(partition)
	defer partitionFile.Close()

	processRecords := func(wg *sync.WaitGroup, records [][]string) {
		defer wg.Done()
		p.transformer.Transform(records) // Layer 3
	}

	var records [][]string

	reader := csv.NewReader(partitionFile)
	reader.Comma = delimiter

	record, err := reader.Read()
	if err != nil {
		panic(err)
	}

	// Skip csv header (if present)
	recordSet := mapset.NewSet(record...)
	if !recordSet.Equal(schema.Header) {
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

		if len(records) == segmentSize {
			wg.Add(1)
			go processRecords(wg, copySlice(records)) // Copy slice for goroutine
			records = records[:0]                     // Reset batch window
		}
	}
}
