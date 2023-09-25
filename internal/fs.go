package internal

import (
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
}

func NewFS(filePath string, partitionDir string, outputDir string) FS {
	filename := GetFileName(filePath)
	hashedFilename := GenerateHash(filename)
	partitionPath := fmt.Sprintf("%s/%s", partitionDir, hashedFilename)
	outputPath := fmt.Sprintf("%s/%s", outputDir, hashedFilename)

	return FS{
		filePath,
		filename,
		hashedFilename,
		partitionPath,
		outputPath,
	}
}

func (f FS) PartitionFile(partitionSize int, batchSize int) (int, chan string) {
	if !PathExists(f.filePath) {
		panic("file doesn't exist")
	}

	lineCount := countLinesInFile(f.filePath)
	totalPartitions := lineCount / partitionSize

	if !PathExists(f.partitionPath) {
		MakeDirectory(f.partitionPath)

		MeasureExecTime("partitioning finished", func() {
			fmt.Printf("partitioning %s into %d partitions of size %d\n", f.filename, totalPartitions, partitionSize)

			cmd := exec.Command(
				"split", "-l",
				fmt.Sprint(partitionSize), f.filePath,
				fmt.Sprintf("%s/partition-", f.partitionPath),
			)

			if _, err := cmd.Output(); err != nil {
				panic(err)
			}
		})
	} else {
		fmt.Println("partitions found")
	}

	partitionsPaths := f.GetPartitions()
	totalBatches := len(partitionsPaths)/batchSize + len(partitionsPaths)%batchSize

	fmt.Printf("divided %d partitions into %d batches of size %d each\n", len(partitionsPaths), totalBatches, batchSize)

	var partitions = make(chan string)
	go func() {
		for _, partition := range partitionsPaths {
			partitions <- partition
		}

		close(partitions)
	}()

	return len(partitionsPaths), partitions
}

func (f FS) OpenPartitionFile(partition string) *os.File {
	partitionPath := fmt.Sprintf("%s/%s", f.partitionPath, partition)
	file, err := os.Open(partitionPath)
	if err != nil {
		panic(err)
	}

	return file
}

func (f FS) GetPartitions() []string {
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

func MakeDirectory(path string) {
	if err := os.MkdirAll(path, 0777); err != nil {
		panic(err)
	}
}

func GetFileName(filePath string) string {
	parts := strings.Split(filePath, "/")
	return parts[len(parts)-1]
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
