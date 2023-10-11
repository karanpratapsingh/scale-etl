package internal

import (
	"bufio"
	"bytes"
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
	noHeader    bool
}

type BatchProcessor interface {
	SegmentProcessor
	BatchComplete(batchNo int)
}

type SegmentProcessor interface {
	ProcessSegment(batchNo int, rows []Row)
}

func NewProcessor(partitioner Partitioner, schema Schema, batchSize int, segmentSize int, delimiter rune, noHeader bool) Processor {
	var wg sync.WaitGroup
	return Processor{partitioner, &wg, schema, batchSize, segmentSize, delimiter, noHeader}
}

func (p *Processor) ProcessPartitions(partitions chan Partition, totalPartitions int, batchProcessor BatchProcessor) {
	MeasureExecTime("Processing complete", func() {
		batchSize := p.batchSize

		for i := 0; i < totalPartitions; i += batchSize {
			batchNo := p.CountBatches(i) + 1
			end := min(totalPartitions, i+batchSize) // Last batch can be less than batchSize

			MeasureExecTime(fmt.Sprintf("Processed batch %d", batchNo), func() {
				for j := i; j < end; j += 1 {
					partition := <-partitions
					partitionNo := j + 1

					p.wg.Add(1)
					go p.processPartition(batchNo, partitionNo, partition, batchProcessor)
				}
				p.wg.Wait()
				batchProcessor.BatchComplete(batchNo)
			})
		}
	})
}

func (p Processor) processPartition(batchNo int, partitionNo int, partition Partition, batchProcessor BatchProcessor) {
	defer p.wg.Done()

	partitionFile := p.partitioner.getInputFile()
	defer partitionFile.Close()

	processRows := func(wg *sync.WaitGroup, rows []Row) {
		defer wg.Done()
		batchProcessor.ProcessSegment(batchNo, rows)
	}

	if _, err := partitionFile.Seek(int64(partition.Start), 0); err != nil {
		panic(err)
	}

	bufferReader := bufio.NewReader(partitionFile)
	rawTxt := make([]byte, partition.End-partition.Start)

	if _, err := bufferReader.Read(rawTxt); err != nil {
		panic(err)
	}

	reader := csv.NewReader(bytes.NewReader(rawTxt))
	reader.Comma = p.delimiter

	var rows []Row

	// Skip csv header from first partition (if present)
	if !p.noHeader && partitionNo == 1 {
		header, err := reader.Read()
		if err != nil {
			panic(err)
		}

		rowSet := mapset.NewSet(header...)
		if !rowSet.Equal(p.schema.Header) {
			panic(ErrUnexpectedNonHeaderRow)
		}

		fmt.Printf("Skipping header %s\n", header)
	}

	for {
		row, err := reader.Read()

		if err == io.EOF {
			// Process the remaining rows
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
			rows = rows[:0]                       // Reset segment window
		}
	}
}

func (p Processor) CountBatches(n int) int {
	return n/p.batchSize + n%p.batchSize
}
