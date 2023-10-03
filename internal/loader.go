package internal

import (
	"fmt"
	"os"
	"os/exec"
	"sync"
)

type Loader struct {
	scriptPath string
	outputPath string
}

func NewLoader(filePath string, scriptPath string, outputDir string) Loader {
	filename := getFileName(filePath)
	hashedFilename := generateHash(filename)
	outputPath := joinPaths(outputDir, hashedFilename)

	return Loader{scriptPath, outputPath}
}

func (l Loader) LoadSegments(poolSize int) error {
	var wg sync.WaitGroup

	printLoaderInfo(poolSize, l.scriptPath)

	MeasureExecTime("Loading complete", func() {
		for batchNo, batchPath := range l.getBatchPaths() {
			segmentChunks := chunk(l.getSegmentPaths(batchPath), poolSize)

			MeasureExecTime(fmt.Sprintf("Loaded batch %d", batchNo+1), func() {
				for _, segmentChunk := range segmentChunks {
					for _, segmentPath := range segmentChunk {

						wg.Add(1)
						go func(wg *sync.WaitGroup, batchPath string, segmentPath string) {
							defer wg.Done()

							l.LoadSegment(batchPath, segmentPath)
						}(&wg, batchPath, segmentPath)
					}
					wg.Wait() // Wait for each chunk to finish
				}
			})
		}
	})

	return nil
}

func (l Loader) LoadSegment(batchPath, segmentPath string) {
	cmd := exec.Command(l.scriptPath, joinPaths(l.outputPath, batchPath, segmentPath))

	if err := cmd.Run(); err != nil {
		fmt.Println(ErrSegmentLoadFailed(segmentPath, err))
	}
}

func (l Loader) getBatchPaths() []string {
	file, err := os.Open(l.outputPath)
	if err != nil {
		panic(ErrBatchesNotFound(err))
	}
	defer file.Close()

	return getFilesInDir(file)
}

func (l Loader) getSegmentPaths(batchPath string) []string {
	file, err := os.Open(joinPaths(l.outputPath, batchPath))
	if err != nil {
		panic(err)
	}
	defer file.Close()

	return getFilesInDir(file)
}
