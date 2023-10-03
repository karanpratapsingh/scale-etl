package internal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type PartitionInfo struct {
	TotalRows  int         `json:"total_rows"`
	Partitions []Partition `json:"partitions"`
}

type Partition struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

type Partitioner struct {
	filePath     string
	filename     string
	infoFilePath string
}

func NewPartitioner(filePath string, partitionDir string) Partitioner {
	if !pathExists(filePath) {
		panic(ErrFileNotFound(filePath))
	}

	makeDirectory(partitionDir)

	filename := getFileName(filePath)
	hashedFilename := generateHash(filename)
	infoFilePath := fmt.Sprintf("%s/%s.json", partitionDir, hashedFilename)

	return Partitioner{filePath, filename, infoFilePath}
}

func (pt Partitioner) PartitionFile(partitionSize int) (err error) {
	MeasureExecTime("Partitioning complete", func() {
		fmt.Printf("Writing partition info for %s at %s\n", pt.filename, pt.infoFilePath)
		err = pt.createPartitions(partitionSize)
	})

	if err != nil {
		return err
	}

	info := pt.GetPartitionsInfo()
	totalPartitions := len(info.Partitions)

	PrintInputFileInfo(pt.filePath, info.TotalRows)
	printPartitionInfo(totalPartitions, partitionSize)

	return nil
}

func (pt Partitioner) StreamPartitions() (chan Partition, int) {
	partitions := pt.GetPartitionsInfo().Partitions
	totalPartitions := len(partitions)

	var partitionsChan = make(chan Partition)
	go func() {
		for _, partition := range partitions {
			partitionsChan <- partition
		}

		close(partitionsChan)
	}()

	return partitionsChan, totalPartitions
}

func (pt Partitioner) CleanPartitions() error {
	err := os.RemoveAll(pt.infoFilePath)
	if err == nil {
		fmt.Println("Cleaned partition info file:", pt.infoFilePath)
	}
	return err
}

func (pt Partitioner) GetPartitionsInfo() PartitionInfo {
	file, err := os.Open(pt.infoFilePath)
	if err != nil {
		panic(ErrPartitionsNotFound(err))
	}
	defer file.Close()

	var partitionInfo PartitionInfo
	if err := json.NewDecoder(file).Decode(&partitionInfo); err != nil {
		panic(err)
	}

	return partitionInfo
}

func (pt Partitioner) createPartitions(partitionSize int) error {
	file, err := os.Open(pt.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	partitions := make([]Partition, 0)
	size := 0

	totalRows := 0
	start := 0
	end := 0

	for {
		line, err := reader.ReadBytes('\n')

		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		totalRows += 1
		end += len(line)
		size += 1

		if size >= partitionSize {
			partitions = append(partitions, Partition{start, end})
			size = 0
			start = end
		}
	}

	// Append partition for remaining lines
	if size > 0 {
		partitions = append(partitions, Partition{start, end})
	}

	if err := checkPartitionSize(partitionSize, totalRows); err != nil {
		return err
	}

	return pt.writePartitionInfoFile(PartitionInfo{totalRows, partitions})
}

func (pt Partitioner) writePartitionInfoFile(info PartitionInfo) error {
	jsonData, err := json.Marshal(info)
	if err != nil {
		return err
	}

	file, err := os.Create(pt.infoFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(jsonData)
	return err
}

func (pt Partitioner) getInputFile() *os.File {
	file, err := os.Open(pt.filePath)
	if err != nil {
		panic(err)
	}

	return file
}
