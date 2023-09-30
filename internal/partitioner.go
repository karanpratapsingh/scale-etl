package internal

import (
	"bufio"
	"fmt"
	"os"
)

type Partitioner struct {
	filePath      string
	filename      string
	partitionPath string
}

func NewPartitioner(filePath string, partitionDir string) Partitioner {
	if !pathExists(filePath) {
		panic(ErrFileNotFound(filePath))
	}

	filename := getFileName(filePath)
	hashedFilename := generateHash(filename)
	partitionPath := fmt.Sprintf("%s/%s", partitionDir, hashedFilename)

	return Partitioner{filePath, filename, partitionPath}
}

func (pt Partitioner) PartitionFile(partitionSize int) (err error) {
	if !pathExists(pt.partitionPath) {
		makeDirectory(pt.partitionPath)

		MeasureExecTime("Partitioning complete", func() {
			fmt.Printf("Partitioning %s in directory %s\n", pt.filename, pt.partitionPath)
			err = pt.createPartitions(partitionSize)
		})
	} else {
		fmt.Println("Found partitions for", pt.filename)
	}

	totalPartitions := len(pt.getPartitions())
	printPartitionInfo(totalPartitions, partitionSize)

	return err
}

func (pt Partitioner) LoadPartitions() (chan string, int) {
	partitionsPaths := pt.getPartitions()
	totalPartitions := len(partitionsPaths)

	var partitions = make(chan string)
	go func() {
		for _, partition := range partitionsPaths {
			partitions <- partition
		}

		close(partitions)
	}()

	return partitions, totalPartitions
}

func (pt Partitioner) getPartitions() []string {
	file, err := os.Open(pt.partitionPath)
	if err != nil {
		fmt.Println("Partitions not found, make sure to run the partition command first.")
		panic(err)
	}
	defer file.Close()

	// Edge case: Readdirnames doesn't distinguish between files and directories
	filenames, err := file.Readdirnames(-1)
	if err != nil {
		panic(err)
	}

	return filenames
}

func (pt Partitioner) createPartitions(partitionSize int) error {
	file, err := os.Open(pt.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	partitionCount := 1
	lines := 0

	outputFile := pt.createPartitionFile(fmt.Sprint(partitionCount))
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)

	for scanner.Scan() {
		line := scanner.Text()
		_, err := fmt.Fprintln(writer, line)
		if err != nil {
			panic(err)
		}

		lines++
		if lines >= partitionSize {
			// Close current partition
			writer.Flush()
			lines = 0
			partitionCount++
			outputFile.Close()

			// Start new partition
			outputFile = pt.createPartitionFile(fmt.Sprint(partitionCount))
			defer outputFile.Close()

			writer = bufio.NewWriter(outputFile)
		}
	}

	return writer.Flush()
}

func (pt Partitioner) createPartitionFile(partition string) *os.File {
	partitionPath := fmt.Sprintf("%s/%s", pt.partitionPath, partition)

	file, err := os.Create(partitionPath)
	if err != nil {
		panic(err)
	}
	return file
}

func (pt Partitioner) getPartitionFile(partition string) *os.File {
	partitionPath := fmt.Sprintf("%s/%s", pt.partitionPath, partition)

	file, err := os.Open(partitionPath)
	if err != nil {
		panic(err)
	}

	return file
}

func (pt Partitioner) CleanPartitions() error {
	err := os.RemoveAll(pt.partitionPath)
	if err == nil {
		fmt.Println("Cleaned partitions directory:", pt.partitionPath)
	}
	return err
}
