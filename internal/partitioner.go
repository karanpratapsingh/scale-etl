package internal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type PartitionManifest struct { // rename and add partition size
	TotalRows     int         `json:"total_rows"`
	PartitionSize int         `json:"partition_size"`
	Partitions    []Partition `json:"partitions"`
}

type Partition struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

type Partitioner struct {
	filePath         string
	filename         string
	manifestFilePath string
}

func NewPartitioner(filePath string, partitionDir string) Partitioner {
	makeDirectory(partitionDir)

	filename := getFileName(filePath)
	hashedFilename := generateHash(filename)
	manifestFilePath := fmt.Sprintf("%s/%s.json", partitionDir, hashedFilename)

	return Partitioner{filePath, filename, manifestFilePath}
}

func (pt Partitioner) PartitionFile(partitionSize int) (err error) {
	MeasureExecTime("Partitioning complete", func() {
		fmt.Printf("Writing partition manifest for %s at %s\n", pt.filename, pt.manifestFilePath)
		err = pt.createPartitionManifest(partitionSize)
	})

	if err != nil {
		return err
	}

	manifest := pt.GetPartitionManifest()
	totalPartitions := len(manifest.Partitions)

	PrintInputFileInfo(pt.filePath, manifest.TotalRows)
	printPartitionInfo(totalPartitions, partitionSize)

	return nil
}

func (pt Partitioner) StreamPartitions() (chan Partition, int) {
	partitions := pt.GetPartitionManifest().Partitions
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
	err := os.RemoveAll(pt.manifestFilePath)
	if err == nil {
		fmt.Println("Cleaned partition manifest file:", pt.manifestFilePath)
	}
	return err
}

func (pt Partitioner) GetPartitionManifest() PartitionManifest {
	file, err := os.Open(pt.manifestFilePath)
	if err != nil {
		panic(ErrPartitionsNotFound(err))
	}
	defer file.Close()

	var manifest PartitionManifest
	if err := json.NewDecoder(file).Decode(&manifest); err != nil {
		panic(err)
	}

	return manifest
}

func (pt Partitioner) createPartitionManifest(partitionSize int) error {
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

	return pt.writePartitionManifestFile(PartitionManifest{totalRows, partitionSize, partitions})
}

func (pt Partitioner) writePartitionManifestFile(manifest PartitionManifest) error {
	jsonData, err := json.Marshal(manifest)
	if err != nil {
		return err
	}

	file, err := os.Create(pt.manifestFilePath)
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
