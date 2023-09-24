package internal

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func ReadChunks(dirPath string, chunks chan *os.File) {
	MeasureExecTime("reading chunks", func() {
		files, err := os.ReadDir(dirPath)
		if err != nil {
			panic(err)
		}

		for _, file := range files {
			if !file.IsDir() {
				chunk, err := os.Open(dirPath + "/" + file.Name())
				if err != nil {
					panic(err)
				}
				chunks <- chunk
			}
		}

		close(chunks)
	})
}

func SplitFile(filePath string, chunkSize int) string {
	if !CheckPathExists(filePath) {
		panic("file doesn't exist")
	}

	filename := GetFileName(filePath)
	dirPath := "chunks/" + GenerateHash(filename)

	if !CheckPathExists(dirPath) {
		MakeDirectory(dirPath)

		MeasureExecTime("splitting", func() {
			lineCount := countLinesInFile(filePath)
			totalChunks := lineCount / chunkSize

			fmt.Printf("Splitting %s into %d chunks of size %d\n", filePath, totalChunks, chunkSize)
			cmd := exec.Command("split", "-l", fmt.Sprint(chunkSize), filePath, dirPath+"/"+"chunk-")
			if _, err := cmd.Output(); err != nil {
				panic(err)
			}
		})
	} else {
		fmt.Println("chunks found, skipping chunking")
	}

	return dirPath
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

func CheckPathExists(path string) bool {
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
