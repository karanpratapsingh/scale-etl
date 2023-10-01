package internal

import (
	"encoding/csv"
	"fmt"
	"io"
	"sync"

	mapset "github.com/deckarep/golang-set/v2"
)

type Processor struct {
	partitioner Partitioner
	wg          *sync.WaitGroup
	schema      Schema
	batchSize   int
	segmentSize int
	delimiter   rune
}

type BatchProcessor interface {
	SegmentProcessor
	BatchComplete(batchNo int)
}

type SegmentProcessor interface {
	ProcessSegment(batchNo int, rows []Row)
}

func NewProcessor(partitioner Partitioner, schema Schema, batchSize int, segmentSize int, delimiter rune) Processor {
	var wg sync.WaitGroup
	return Processor{partitioner, &wg, schema, batchSize, segmentSize, delimiter}
}

func (p *Processor) ProcessPartitions(partitions chan string, totalPartitions int, batchProcessor BatchProcessor) {
	MeasureExecTime("Processing complete", func() {
		batchSize := p.batchSize

		for i := 0; i < totalPartitions; i += batchSize {
			batchNo := p.CountBatches(i) + 1
			end := min(totalPartitions, i+batchSize) // Last batch can be less than batchSize

			MeasureExecTime(fmt.Sprintf("Processed batch %d", batchNo), func() {
				for j := i; j < end; j += 1 {
					partition := <-partitions

					p.wg.Add(1)
					go p.processPartition(batchNo, partition, batchProcessor)
				}
				p.wg.Wait()
				batchProcessor.BatchComplete(batchNo)
			})
		}
	})
}

func (p Processor) processPartition(batchNo int, partition string, batchProcessor BatchProcessor) {
	defer p.wg.Done()

	partitionFile := p.partitioner.getPartitionFile(partition)
	defer partitionFile.Close()

	processRows := func(wg *sync.WaitGroup, rows []Row) {
		defer wg.Done()
		batchProcessor.ProcessSegment(batchNo, rows)
	}

	var rows []Row

	reader := csv.NewReader(partitionFile)
	reader.Comma = p.delimiter

	row, err := reader.Read()
	if err != nil {
		panic(err)
	}

	// Skip csv header (if present)
	rowSet := mapset.NewSet(row...)
	if !rowSet.Equal(p.schema.Header) {
		rows = append(rows, row)
	}

	for {
		row, err := reader.Read()

		if err == io.EOF {
			// Remaining rows when window size is less than batch size
			if len(rows) != 0 {
				p.wg.Add(1)
				go processRows(p.wg, copySlice(rows))
			}
			break
		} else if err != nil {
			panic(err)
		}

		rows = append(rows, row)

		if len(rows) == p.segmentSize {
			p.wg.Add(1)
			go processRows(p.wg, copySlice(rows)) // Copy slice for goroutine
			rows = rows[:0]                       // Reset batch window
		}
	}
}

func (p Processor) CountBatches(n int) int {
	return n/p.batchSize + n%p.batchSize
}
