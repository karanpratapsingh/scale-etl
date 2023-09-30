package internal

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func CountFileRows(path string) int {
	cmd := exec.Command("wc", "-l", path)

	output, err := cmd.Output()
	if err != nil {
		panic(ErrCountingRows(path, err))
	}

	parts := strings.Fields(string(output))

	lineCount, err := strconv.Atoi(parts[0])
	if err != nil {
		panic(err)
	}

	return lineCount
}

func getSearchResultsFile(path string) *os.File {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644) // Append only
	if err != nil {
		panic(err)
	}
	return file
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
