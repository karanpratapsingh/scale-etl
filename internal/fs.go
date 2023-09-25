package internal

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// TODO: [][]fs.DirEntry -> paths?
func ReadPartitionBatches(dirPath string, partitionBatches [][]fs.DirEntry, partitions chan *os.File, processed chan struct{}) {
	for _, batch := range partitionBatches {
		for _, file := range batch {
			if !file.IsDir() {
				partition, err := os.Open(fmt.Sprintf("%s/%s", dirPath, file.Name()))
				if err != nil {
					panic(err)
				}
				partitions <- partition
			}
		}
		<-processed
	}

	close(partitions)
}

func PartitionFile(filePath string, partitionDir string, partitionSize int, batchSize int) (string, [][]fs.DirEntry) {
	if !PathExists(filePath) {
		panic("file doesn't exist")
	}

	filename := GetFileName(filePath)
	dirPath := fmt.Sprintf("%s/%s", partitionDir, GenerateHash(filename))

	lineCount := countLinesInFile(filePath)
	totalPartitions := lineCount / partitionSize

	if !PathExists(dirPath) {
		MakeDirectory(dirPath)

		MeasureExecTime("splitting", func() {
			fmt.Printf("splitting %s into %d partitions of size %d\n", filePath, totalPartitions, partitionSize)

			cmd := exec.Command("split", "-l", fmt.Sprint(partitionSize), filePath, fmt.Sprintf("%s/partition-", dirPath))
			if _, err := cmd.Output(); err != nil {
				panic(err)
			}
		})
	} else {
		fmt.Println("partitions found")
	}

	files, err := os.ReadDir(dirPath)
	if err != nil {
		panic(err)
	}

	partitionBatches := makeBatches(files, batchSize)
	fmt.Printf("divided %d partitions into %d batches\n", totalPartitions, len(partitionBatches))

	return dirPath, partitionBatches
}

func MakeDirectory(path string) {
	if err := os.MkdirAll(path, 0777); err != nil {
		panic(err)
	}
}

func GetFileName(filePath string) string {
	parts := strings.Split(filePath, ".")

	return parts[0]
}

func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return false
	}

	return true
}

func countLinesInFile(filePath string) int {
	cmd := exec.Command("wc", "-l", filePath)

	output, err := cmd.Output()
	if err != nil {
		panic(err)
	}

	parts := strings.Split(string(output), " ")

	lineCount, err := strconv.Atoi(parts[1])
	if err != nil {
		panic(err)
	}

	return lineCount
}
