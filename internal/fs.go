package internal

import (
	"fmt"
	"os"
	"os/exec"
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
	filename := getFileName(filePath)
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
	if !pathExists(f.filePath) {
		panic("file doesn't exist")
	}

	if !pathExists(f.partitionPath) {
		makeDirectory(f.partitionPath)

		MeasureExecTime("partitioning finished", func() {
			fmt.Printf("partitioning %s into partitions of size %d\n", f.filename, partitionSize)

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

	partitionsPaths := f.getPartitions()
	totalPartitions := len(partitionsPaths)
	totalBatches := totalPartitions/batchSize + totalPartitions%batchSize

	if batchSize > totalPartitions {
		panic(fmt.Sprintf("batch size (%d) should be less than total partitions (%d)", batchSize, totalPartitions))
	}

	fmt.Printf("divided %d partitions into %d batches of size %d each\n", totalPartitions, totalBatches, batchSize)

	var partitions = make(chan string)
	go func() {
		for _, partition := range partitionsPaths {
			partitions <- partition
		}

		close(partitions)
	}()

	return totalPartitions, partitions
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

func (f FS) writeFile(path string, extension string, data []byte) {
	filePath := fmt.Sprintf("%s/%s.%s", f.outputPath, path, extension)

	file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	if _, err = file.Write(data); err != nil {
		panic(err)
	}
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

// TODO: use to print info, this crashes for smaller files
// func countLinesInFile(filePath string) int {
// 	cmd := exec.Command("wc", "-l", filePath)

// 	output, err := cmd.Output()
// 	if err != nil {
// 		panic(err)
// 	}

// 	parts := strings.Split(string(output), " ")

// 	lineCount, err := strconv.Atoi(parts[1])
// 	if err != nil {
// 		panic(err)
// 	}

// 	return lineCount
// }
