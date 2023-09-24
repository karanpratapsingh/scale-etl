package internal

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func ReadChunksBuffers(dirPath string, chunksBuffers [][]fs.DirEntry, chunks chan *os.File, processed chan struct{}) {
	for _, buffer := range chunksBuffers {
		for _, file := range buffer {
			if !file.IsDir() {
				chunk, err := os.Open(fmt.Sprintf("%s/%s", dirPath, file.Name()))
				if err != nil {
					panic(err)
				}
				chunks <- chunk
			}
		}
		fmt.Println("sent", buffer)
		<-processed
	}

	close(chunks)
}

func SplitInputFile(filePath string, processDir string, chunkSize int, bufferSize int) (string, [][]fs.DirEntry) {
	if !PathExists(filePath) {
		panic("file doesn't exist")
	}

	filename := GetFileName(filePath)
	dirPath := fmt.Sprintf("%s/%s", processDir, GenerateHash(filename))

	lineCount := countLinesInFile(filePath)
	totalChunks := lineCount / chunkSize

	if !PathExists(dirPath) {
		MakeDirectory(dirPath)

		MeasureExecTime("splitting", func() {

			fmt.Printf("Splitting %s into %d chunks of size %d\n", filePath, totalChunks, chunkSize)

			cmd := exec.Command("split", "-l", fmt.Sprint(chunkSize), filePath, fmt.Sprintf("%s/chunk-", dirPath))
			if _, err := cmd.Output(); err != nil {
				panic(err)
			}
		})
	} else {
		fmt.Println("chunks found, skipping chunking")
	}

	files, err := os.ReadDir(dirPath)
	if err != nil {
		panic(err)
	}

	chunksBuffers := chunk(files, bufferSize)
	fmt.Printf("divided %d chunks into %d buffers\n", totalChunks, len(chunksBuffers))

	return dirPath, chunksBuffers
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
