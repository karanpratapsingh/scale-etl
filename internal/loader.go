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

	segmentPaths := l.getFullSegmentPaths()
	totalSegmentPaths := len(segmentPaths)

	if poolSize > totalSegmentPaths {
		panic(ErrPoolSizeTooLarge(poolSize, totalSegmentPaths))
	}

	segmentPathBatches := createBatches(segmentPaths, poolSize)
	totalBatches := len(segmentPathBatches)

	printLoaderInfo(poolSize, totalSegmentPaths, totalBatches, l.scriptPath)

	MeasureExecTime("Loading complete", func() {
		for batchNo, segmentPathBatch := range segmentPathBatches {
			MeasureExecTime(fmt.Sprintf("Loaded batch %d", batchNo+1), func() {
				for _, segmentPath := range segmentPathBatch {
					wg.Add(1)
					go func(wg *sync.WaitGroup, segmentPath string) {
						defer wg.Done()

						l.LoadSegment(segmentPath)
					}(&wg, segmentPath)
				}
				wg.Wait() // Wait for each batch to finish
			})
		}
	})

	return nil
}

func (l Loader) getFullSegmentPaths() []string {
	batchPaths := l.getBatchPaths()

	var segmentPaths []string

	for _, batchPath := range batchPaths {
		for _, segmentPath := range l.getSegmentPaths(batchPath) {
			fullPath := fmt.Sprintf("%s/%s", batchPath, segmentPath)
			segmentPaths = append(segmentPaths, fullPath)
		}
	}

	return segmentPaths
}

func (l Loader) LoadSegment(segmentPath string) {
	cmd := exec.Command(l.scriptPath, joinPaths(l.outputPath, segmentPath))

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
