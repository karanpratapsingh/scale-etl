package internal

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

type FS struct {
	filePath       string
	filename       string
	hashedFilename string
	partitionPath  string
	outputPath     string
	counter        Counter
}

func NewFS(filePath string, partitionDir string, outputDir string) FS {
	filename := getFileName(filePath)
	hashedFilename := generateHash(filename)
	partitionPath := fmt.Sprintf("%s/%s", partitionDir, hashedFilename)
	outputPath := fmt.Sprintf("%s/%s", outputDir, hashedFilename)
	counter := NewCounter(0)

	return FS{
		filePath,
		filename,
		hashedFilename,
		partitionPath,
		outputPath,
		counter,
	}
}

func (f FS) PartitionFile(partitionSize int) (err error) {
	if !pathExists(f.partitionPath) {
		makeDirectory(f.partitionPath)

		MeasureExecTime("Partitioning complete", func() {
			fmt.Printf("Partitioning %s in directory %s\n", f.filename, f.partitionPath)
			err = f.createPartitions(partitionSize)
		})
	} else {
		fmt.Println("Found partitions for", f.filename)
	}

	totalPartitions := len(f.getPartitions())
	printPartitionInfo(totalPartitions, partitionSize)

	return err
}

func (f FS) LoadPartitions(partitionSize int, batchSize int) (chan string, int, int) {
	partitionsPaths := f.getPartitions()
	totalPartitions := len(partitionsPaths)
	totalBatches := countBatches(totalPartitions, batchSize)

	if batchSize > totalPartitions {
		panic(fmt.Sprintf("batch size (%d) should be less than total partitions (%d)", batchSize, totalPartitions))
	}

	printPartitionInfo(totalPartitions, partitionSize)
	printBatchInfo(totalBatches, batchSize)

	var partitions = make(chan string)
	go func() {
		for _, partition := range partitionsPaths {
			partitions <- partition
		}

		close(partitions)
	}()

	return partitions, totalPartitions, totalBatches
}

func (f FS) CleanPartitions() error {
	err := os.RemoveAll(f.partitionPath)
	if err == nil {
		fmt.Println("Cleaned partitions directory:", f.partitionPath)
	}
	return err
}

func (f FS) createPartitions(partitionSize int) error {
	file, err := os.Open(f.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	partitionCount := 1
	lines := 0

	outputFile := f.createPartitionFile(fmt.Sprint(partitionCount))
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
			outputFile = f.createPartitionFile(fmt.Sprint(partitionCount))
			defer outputFile.Close()

			writer = bufio.NewWriter(outputFile)
		}
	}

	return writer.Flush()
}

func (f FS) createPartitionFile(partition string) *os.File {
	partitionPath := fmt.Sprintf("%s/%s", f.partitionPath, partition)

	file, err := os.Create(partitionPath)
	if err != nil {
		panic(err)
	}
	return file
}

func (f FS) getPartitionFile(partition string) *os.File {
	partitionPath := fmt.Sprintf("%s/%s", f.partitionPath, partition)

	file, err := os.Open(partitionPath)
	if err != nil {
		panic(err)
	}

	return file
}

func (f FS) getPartitions() []string {
	file, err := os.Open(f.partitionPath)
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

func (f FS) writeSegmentFile(batchNo int, data any, extension ExtensionType) {
	filename := f.counter.get()
	filePath := fmt.Sprintf("%s/%d/%d.%s", f.outputPath, batchNo, filename, extension)

	file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	switch extension {
	case ExtensionTypeJSON:
		writer := bufio.NewWriter(file)
		defer writer.Flush()

		if _, err = writer.Write(data.([]byte)); err != nil {
			panic(err)
		}
	case ExtensionTypeCSV:
		writer := csv.NewWriter(file)
		defer writer.Flush()

		if err := writer.WriteAll(data.([]Row)); err != nil {
			panic(err)
		}
	}
}

func (f FS) getSearchResultsFile(path string) *os.File {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644) // Append only
	if err != nil {
		panic(err)
	}
	return file
}

func countFileRows(path string) int {
	cmd := exec.Command("wc", "-l", path)

	output, err := cmd.Output()
	if err != nil {
		panic(fmt.Errorf("failed to count rows for file %s: %v", path, err))
	}

	parts := strings.Fields(string(output))

	lineCount, err := strconv.Atoi(parts[0])
	if err != nil {
		panic(err)
	}

	return lineCount
}

func getFileSize(path string) float64 {
	fileInfo, err := os.Stat(path)
	if err != nil {
		panic(err)
	}

	return float64(fileInfo.Size()) / (1024 * 1024) // MB
}

func makeDirectory(path string) {
	if err := os.MkdirAll(path, os.FileMode(0777)); err != nil {
		panic(err)
	}
}

func getFileName(filePath string) string {
	parts := strings.Split(filePath, "/")
	return parts[len(parts)-1]
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return false
	}

	return true
}
