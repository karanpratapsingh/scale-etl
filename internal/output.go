package internal

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
)

type Output struct {
	filePath       string
	filename       string
	hashedFilename string
	outputPath     string
	counter        Counter
}

func NewOutput(filePath string, outputDir string) Output {
	filename := getFileName(filePath)
	hashedFilename := generateHash(filename)
	outputPath := fmt.Sprintf("%s/%s", outputDir, hashedFilename)
	counter := NewCounter(0)

	return Output{
		filePath,
		filename,
		hashedFilename,
		outputPath,
		counter,
	}
}

func (o Output) PrepareOutputDirs(totalBatches int) {
	// Delete existing output directory
	if pathExists(o.outputPath) {
		os.RemoveAll(o.outputPath)
	}

	// Create the directories for batches
	for i := 1; i <= totalBatches; i += 1 {
		makeDirectory(fmt.Sprintf("%s/%d", o.outputPath, i))
	}
}

func (o Output) getSegmentFilePath(batchNo int, extension ExtensionType) string {
	filename := o.counter.get()
	filePath := fmt.Sprintf("%s/%d/%d.%s", o.outputPath, batchNo, filename, extension)

	return filePath
}

func (o Output) writeSegmentFile(batchNo int, data any, extension ExtensionType) {
	filePath := o.getSegmentFilePath(batchNo, extension)

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

		if err := writer.WriteAll(data.([]Row)); err != nil {
			panic(err)
		}
	}
}

// func (f FS) getSearchResultsFile(path string) *os.File {
// 	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644) // Append only
// 	if err != nil {
// 		panic(err)
// 	}
// 	return file
// }
