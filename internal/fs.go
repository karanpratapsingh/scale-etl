package internal

import (
	"fmt"
	"os"
	"os/exec"
)

func ReadChunks(dirPath string, chunks chan *os.File) {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		fmt.Println(err)
		return
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
}

func SplitFile(filename string, chunkSize int) string { // Create dir (if doesn't exists)
	dirPath := "chunks/" + filename

	if !dirExists(dirPath) {
		if err := os.MkdirAll("chunks/"+filename, 0777); err != nil {
			panic(err)
		}

		cmd := exec.Command("split", "-l", fmt.Sprint(chunkSize), filename+".csv", dirPath+"/chunk-")
		if _, err := cmd.Output(); err != nil {
			panic(err)
		}
	}

	return dirPath
}

func dirExists(dirPath string) bool {
	_, err := os.Stat(dirPath)
	if err != nil && os.IsNotExist(err) {
		return false
	}

	return true
}
