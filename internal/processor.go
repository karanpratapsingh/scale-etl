package internal

import (
	"encoding/csv"
	"fmt"
	"io"
	"sync"

	mapset "github.com/deckarep/golang-set/v2"
)

type Processor struct {
	fs          FS
	wg          *sync.WaitGroup
	schema      Schema
	batchSize   int
	segmentSize int
	delimiter   rune
}

func NewProcessor(fs FS, schema Schema, batchSize int, segmentSize int, delimiter rune) Processor {
	printSegmentInfo(segmentSize)

	var wg sync.WaitGroup
	return Processor{fs, &wg, schema, batchSize, segmentSize, delimiter}
}

func (p *Processor) ProcessPartitions(totalPartitions int, partitions chan string, processSegment func(int, [][]string)) {
	MeasureExecTime("Processing complete", func() {
		batchSize := p.batchSize

		for i := 0; i < totalPartitions; i += batchSize {
			batchNo := countBatches(i, batchSize) + 1
			end := min(totalPartitions, i+batchSize) // Last batch can be less than batchSize

			MeasureExecTime(fmt.Sprintf("Processed batch %d", batchNo), func() {
				for j := i; j < end; j += 1 {
					partition := <-partitions

					p.wg.Add(1)
					go p.processBatch(batchNo, partition, processSegment)
				}
				p.wg.Wait()
			})
		}
	})
}

func (p Processor) processBatch(batchNo int, partition string, processSegment func(int, [][]string)) {
	defer p.wg.Done()

	partitionFile := p.fs.openPartitionFile(partition)
	defer partitionFile.Close()

	processRecords := func(wg *sync.WaitGroup, records [][]string) {
		defer wg.Done()
		processSegment(batchNo, records)
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
				p.wg.Add(1)
				go processRecords(p.wg, copySlice(records))
			}
			break
		} else if err != nil {
			panic(err)
		}

		records = append(records, record)

		if len(records) == p.segmentSize {
			p.wg.Add(1)
			go processRecords(p.wg, copySlice(records)) // Copy slice for goroutine
			records = records[:0]                       // Reset batch window
		}
	}
}

func countBatches(n int, batchSize int) int {
	return n/batchSize + n%batchSize
}
