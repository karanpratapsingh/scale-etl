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

func (f FS) PartitionFile(partitionSize int, batchSize int) (int, int, chan string) {
	if !pathExists(f.partitionPath) {
		makeDirectory(f.partitionPath)

		MeasureExecTime("Partitioning complete", func() {
			fmt.Printf("Partitioning %s at %s\n", f.filename, f.partitionPath)

			cmd := exec.Command(
				"split", "-l",
				fmt.Sprint(partitionSize), f.filePath,
				fmt.Sprintf("%s/partition-", f.partitionPath),
			)

			if err := cmd.Start(); err != nil {
				panic(err)
			}

			if err := cmd.Wait(); err != nil {
				panic(err)
			}
		})
	} else {
		fmt.Println("Found partitions for", f.filename)
	}

	partitionsPaths := f.getPartitions()
	totalPartitions := len(partitionsPaths)
	totalBatches := CountBatches(totalPartitions, batchSize)

	if batchSize > totalPartitions {
		panic(fmt.Sprintf("batch size (%d) should be less than total partitions (%d)", batchSize, totalPartitions))
	}

	printPartitionInfo(totalPartitions, partitionSize, totalBatches, batchSize)

	var partitions = make(chan string)
	go func() {
		for _, partition := range partitionsPaths {
			partitions <- partition
		}

		close(partitions)
	}()

	return totalPartitions, totalBatches, partitions
}

func CountBatches(n int, batchSize int) int {
	return n/batchSize + n%batchSize
}

func (f FS) openPartitionFile(partition string) *os.File {
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

func (f FS) writeSegmentFile(batchNo int, extension ExtensionType, data any) {
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

		if err := writer.WriteAll(data.([][]string)); err != nil {
			panic(err)
		}
	}

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
	if err := os.MkdirAll(path, 0777); err != nil {
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
